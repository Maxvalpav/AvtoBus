using System.Security.Cryptography;
using AvtoBus;

namespace AvtoBus.Security;

/// <summary>
/// Полновесная защита конвертов (идеи 451, 452, 455, 459):
/// HMAC-SHA256 подпись всех входящих/исходящих сообщений, опциональное шифрование тела
/// (AES-256-GCM), автоматическое обновление ключей поверх мастер-секрета и rate limit
/// на исходящий трафик. Подключается через <c>configurator.EnvelopeSecurity = ...</c>.
/// </summary>
public sealed class EnvelopeSecurity : IEnvelopeSecurity
{
    private readonly SecurityOptions _options;
    private readonly KeyRing _keys;
    private readonly RateLimiter _outbound;

    public EnvelopeSecurity(SecurityOptions options)
    {
        _options = options;
        _keys = new KeyRing(options);
        _outbound = new RateLimiter(options.OutboundRatePerSecond);
        IsEnabled = options.RequireSignature || options.EncryptBody || options.OutboundRatePerSecond > 0;
    }

    public bool IsEnabled { get; }

    /// <summary>Точка ротации: вызывается hosted service'ом SecurityKeyRotationService по расписанию.</summary>
    public void RotateKeysIfDue(DateTimeOffset now) => _keys.RotateIfDue(now);

    public Envelope ProtectOutbound(Envelope envelope, string? serviceIdentity)
    {
        _outbound.WaitIfNeeded();

        var key = _keys.Actual;
        Envelope prepared = envelope;

        if (_options.EncryptBody)
        {
            prepared = prepared with
            {
                Body = BodyEncryptor.Encrypt(
                    envelope.Body.Span,
                    key.EncryptionKey,
                    out var nonceBase64),
            };
            prepared = prepared.WithHeader(BodyEncryptor.NonceHeader, nonceBase64);
        }

        if (_options.RequireSignature)
        {
            var signature = EnvelopeSigner.ComputeSignature(prepared, key.SigningKey);
            prepared = prepared
                .WithHeader(EnvelopeSigner.SignatureHeader, signature)
                .WithHeader(EnvelopeSigner.SignedByHeader, serviceIdentity ?? _options.SigningIdentity);
        }

        return prepared;
    }

    public Envelope OpenInbound(Envelope envelope)
    {
        if (envelope.Header(EnvelopeSigner.SignatureHeader) is not null || _options.RequireSignature)
        {
            if (envelope.Header(EnvelopeSigner.SignatureHeader) is null)
                throw new SecurityViolationException("Отсутствует подпись, но RequireSignature включён");

            var ok = _keys.TryVerify(
                envelope,
                static (env, key) => EnvelopeSigner.Verify(env, key),
                out _);

            if (!ok)
                throw new SecurityViolationException("Неверная подпись конверта");
        }

        if (_options.EncryptBody && BodyEncryptor.IsEncrypted(envelope))
        {
            if (!BodyEncryptor.TryReadNonce(envelope, out var nonce))
                throw new SecurityViolationException("Повреждён заголовок шифрования (nonce)");

            foreach (var keys in _keys.AllGenerationsOrderedDesc())
            {
                try
                {
                    return envelope with
                    {
                        Body = BodyEncryptor.Decrypt(envelope.Body.Span, keys.EncryptionKey, nonce),
                    };
                }
                catch (CryptographicException)
                {
                    // try next generation
                }
            }

            throw new SecurityViolationException("Не удалось расшифровать тело конверта ни одним из поколений ключей");
        }

        return envelope;
    }
}

/// <summary>
/// Простой точный limiter (идея 459): не более N сообщений в секунду. При нулевом N — пропускает всё.
/// Потокобезопасен; при превышении лимита короткая асинхронная задержка вместо бригады исключений.
/// </summary>
internal sealed class RateLimiter(int permitsPerSecond)
{
    private int _counter;
    private long _windowStartTicks;

    public void WaitIfNeeded()
    {
        if (permitsPerSecond <= 0)
            return;

        long waitMs = 0;
        lock (this)
        {
            var nowTicks = Environment.TickCount64;
            if (nowTicks - _windowStartTicks >= 1000)
            {
                _windowStartTicks = nowTicks;
                _counter = 0;
            }

            if (_counter >= permitsPerSecond)
                waitMs = 1000 - (nowTicks - _windowStartTicks);

            _counter++;
        }

        if (waitMs > 0)
            Thread.Sleep((int)waitMs);
    }

    public ValueTask WaitIfNeededAsync(CancellationToken ct = default)
    {
        if (permitsPerSecond <= 0)
            return ValueTask.CompletedTask;

        long waitMs = 0;
        lock (this)
        {
            var nowTicks = Environment.TickCount64;
            if (nowTicks - _windowStartTicks >= 1000)
            {
                _windowStartTicks = nowTicks;
                _counter = 0;
            }

            if (_counter >= permitsPerSecond)
                waitMs = 1000 - (nowTicks - _windowStartTicks);

            _counter++;
        }

        return waitMs > 0
            ? new ValueTask(Task.Delay((int)waitMs, ct))
            : ValueTask.CompletedTask;
    }
}
