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
            // AAD = MessageId: шифротекст привязан к конверту, перестановка тела
            // в другое сообщение ломает расшифровку даже без проверки подписи.
            Span<byte> aad = stackalloc byte[16];
            envelope.MessageId.TryWriteBytes(aad);
            prepared = prepared with
            {
                Body = BodyEncryptor.Encrypt(
                    envelope.Body.Span,
                    key.EncryptionKey,
                    out var nonceBase64,
                    aad),
            };
            prepared = prepared.WithHeader(BodyEncryptor.NonceHeader, nonceBase64);
        }

        if (_options.RequireSignature)
        {
            prepared = StampSignature(prepared, key, serviceIdentity);
        }

        return prepared;
    }

    /// <summary>
    /// Ставит свежую подпись схемой из <see cref="SecurityOptions.SignatureVersion"/>
    /// (v2 по умолчанию). Отдельно от <c>ProtectCore</c> — переиспользуется после
    /// расшифровки (см. <c>OpenInbound</c>).
    /// </summary>
    internal Envelope StampSignature(Envelope envelope, SecurityKeys keys, string? serviceIdentity)
    {
        var version = _options.SignatureVersion >= EnvelopeSigner.V2 ? EnvelopeSigner.V2 : EnvelopeSigner.V1;
        var signature = EnvelopeSigner.ComputeSignature(envelope, keys.SigningKey, version);
        var stamped = envelope
            .WithHeader(EnvelopeSigner.SignatureHeader, signature)
            .WithHeader(EnvelopeSigner.SignedByHeader, serviceIdentity ?? _options.SigningIdentity);
        if (version >= EnvelopeSigner.V2)
            stamped = stamped.WithHeader(EnvelopeSigner.SignatureVersionHeader, "2");
        return stamped;
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
                    Span<byte> aad = stackalloc byte[16];
                    envelope.MessageId.TryWriteBytes(aad);
                    var plain = BodyEncryptor.Decrypt(envelope.Body.Span, keys.EncryptionKey, nonce, aad);
                    // Strip nonce header after successful decrypt to avoid header leak
                    var strippedHeaders = envelope.Headers.Where(kv => kv.Key != BodyEncryptor.NonceHeader)
                        .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
                    var opened = envelope with
                    {
                        Body = plain,
                        Headers = System.Collections.Frozen.FrozenDictionary.ToFrozenDictionary(strippedHeaders, StringComparer.Ordinal),
                    };
                    // Подпись была посчитана по шифртексту: на открытом теле она протухает,
                    // и SignedPrincipalExtractor отклонял бы свои же сообщения.
                    // Подпись уже проверена выше — перештамповываем свежую по plaintext.
                    if (opened.Header(EnvelopeSigner.SignatureHeader) is not null)
                        opened = StampSignature(opened, keys, opened.Header(EnvelopeSigner.SignedByHeader));
                    return opened;
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

    public async ValueTask WaitIfNeededAsync(CancellationToken ct = default)
    {
        var ms = Reserve();
        if (ms <= 0) return;
        var jitter = Random.Shared.Next(0, 30);
        await Task.Delay((int)ms + jitter, ct).ConfigureAwait(false);
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
