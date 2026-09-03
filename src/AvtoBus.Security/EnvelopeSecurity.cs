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

    /// <summary>
    /// Проверяет подпись входящего конверта всеми поколениями ключей, не расшифровывая тело.
    /// Используется <see cref="SignedPrincipalExtractor"/>: контексту пользователя
    /// (<c>avtobus-user</c>) доверяем только при валидной подписи.
    /// </summary>
    public bool HasValidSignature(Envelope envelope)
    {
        if (envelope.Header(EnvelopeSigner.SignatureHeader) is null)
            return false;
        return _keys.TryVerify(envelope, static (env, key) => EnvelopeSigner.Verify(env, key), out _);
    }

    /// <summary>Точка ротации: вызывается hosted service'ом SecurityKeyRotationService по расписанию.</summary>
    public void RotateKeysIfDue(DateTimeOffset now) => _keys.RotateIfDue(now);

    public Envelope ProtectOutbound(Envelope envelope, string? serviceIdentity)
    {
        _outbound.WaitIfNeeded();
        return ProtectCore(envelope, serviceIdentity);
    }

    public async ValueTask<Envelope> ProtectOutboundAsync(Envelope envelope, string? serviceIdentity, CancellationToken ct = default)
    {
        await _outbound.WaitIfNeededAsync(ct).ConfigureAwait(false);
        return ProtectCore(envelope, serviceIdentity);
    }

    private Envelope ProtectCore(Envelope envelope, string? serviceIdentity)
    {
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
            var version = _options.SignatureVersion >= EnvelopeSigner.V2 ? EnvelopeSigner.V2 : EnvelopeSigner.V1;
            var signature = EnvelopeSigner.ComputeSignature(prepared, key.SigningKey, version);
            prepared = prepared
                .WithHeader(EnvelopeSigner.SignatureHeader, signature)
                .WithHeader(EnvelopeSigner.SignedByHeader, serviceIdentity ?? _options.SigningIdentity);
            if (version >= EnvelopeSigner.V2)
                prepared = prepared.WithHeader(EnvelopeSigner.SignatureVersionHeader, "2");
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
                    var plain = BodyEncryptor.Decrypt(envelope.Body.Span, keys.EncryptionKey, nonce);
                    // Strip nonce header after successful decrypt to avoid header leak
                    var strippedHeaders = envelope.Headers.Where(kv => kv.Key != BodyEncryptor.NonceHeader)
                        .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
                    return envelope with
                    {
                        Body = plain,
                        Headers = System.Collections.Frozen.FrozenDictionary.ToFrozenDictionary(strippedHeaders, StringComparer.Ordinal),
                    };
                }
                catch (Exception ex) when (ex is CryptographicException or ArgumentException)
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
    private readonly object _sync = new();
    private int _counter;
    private long _windowStartTicks;

    public void WaitIfNeeded()
    {
        var ms = Reserve();
        if (ms > 0)
        {
            // jitter 0-30ms распыляет thundering herd (100 потоков не просыпаются в 1ms)
            var jitter = Random.Shared.Next(0, 30);
            Thread.Sleep((int)ms + jitter);
        }
    }

    public ValueTask WaitIfNeededAsync(CancellationToken ct = default)
    {
        var ms = Reserve();
        if (ms <= 0) return ValueTask.CompletedTask;
        var jitter = Random.Shared.Next(0, 30);
        return new ValueTask(Task.Delay((int)ms + jitter, ct));
    }

    private long Reserve()
    {
        if (permitsPerSecond <= 0) return 0;
        lock (_sync)
        {
            var now = Environment.TickCount64;
            if (now - _windowStartTicks >= 1000) { _windowStartTicks = now; _counter = 0; }
            if (_counter >= permitsPerSecond) return 1000 - (now - _windowStartTicks);
            _counter++;
            return 0;
        }
    }
}
