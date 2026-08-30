using System.Collections.Concurrent;
using System.Security.Cryptography;
using AvtoBus;

namespace AvtoBus.Security;

/// <summary>
/// Кольцо ключей с поколениями (идея 452): подпись всегда делается актуальным ключом,
/// проверка — текущим или одним из предыдущих поколений (разрешённая задержка доставки).
/// Поколения детерминированы: выводятся из мастер-секрета индексируемой эпохой функцией —
/// все инстансы кластера, стартовавшие в одном окне, имеют один и тот же ключ.
/// </summary>
public sealed class KeyRing
{
    private readonly int _keepPrevious;
    private readonly SecurityOptions _options;
    private readonly ConcurrentDictionary<long, SecurityKeys> _generations = new();
    private readonly Lock _gate = new();
    private RotationState _current;

    public KeyRing(SecurityOptions options)
    {
        _options = options;
        _keepPrevious = Math.Max(0, options.KeepPreviousKeyGenerations);

        var initialEpoch = EpochOf(DateTimeOffset.UtcNow, options.KeyRotationInterval);
        var initial = _options.Keys.SigningKey.Length > 0
            ? _options.Keys
            : Derive(options, initialEpoch);

        OptionsEpoch = initialEpoch;
        _current = new RotationState(0, initialEpoch, initial);
        _generations[initialEpoch] = initial;
        RefreshSnapshot();
    }

    /// <summary>Текущая эпоха, из которой выводится актуальный ключ.</summary>
    public long OptionsEpoch { get; private set; }

    public SecurityKeys Actual => _current.Keys;

    public void RotateIfDue(DateTimeOffset now)
    {
        if (_options.KeyRotationInterval is not { } interval)
            return;

        var epoch = EpochOf(now, interval);
        if (epoch == OptionsEpoch)
            return;

        var next = Derive(_options, epoch);
        lock (_gate)
        {
            if (epoch == OptionsEpoch) return;
            OptionsEpoch = epoch;
            _current = new RotationState(0, epoch, next);
            _generations[epoch] = next;

            if (_keepPrevious == 0)
            {
                _generations.Clear();
                _generations[epoch] = next;
                return;
            }

            var minKeep = epoch - _keepPrevious;
            foreach (var generation in _generations.Keys.ToArray())
            {
                if (generation < minKeep)
                    _generations.TryRemove(generation, out _);
            }
            RefreshSnapshot();
        }
    }

    private volatile KeyValuePair<long, SecurityKeys>[] _sortedSnapshot = [];

    private void RefreshSnapshot()
    {
        var snapshot = _generations.ToArray();
        Array.Sort(snapshot, (a, b) => b.Key.CompareTo(a.Key));
        _sortedSnapshot = snapshot;
    }

    public bool TryVerify(Envelope envelope, Func<Envelope, ReadOnlySpan<byte>, bool> verify, out long verifiedByEpoch)
    {
        var snapshot = _sortedSnapshot.Length == _generations.Count ? _sortedSnapshot : _generations.ToArray();
        if (snapshot.Length != _generations.Count || _sortedSnapshot.Length == 0)
        {
            snapshot = _generations.ToArray();
            Array.Sort(snapshot, (a, b) => b.Key.CompareTo(a.Key));
        }
        foreach (var (epoch, keys) in snapshot)
        {
            if (verify(envelope, keys.SigningKey))
            {
                verifiedByEpoch = epoch;
                return true;
            }
        }

        verifiedByEpoch = -1;
        return false;
    }

    public IEnumerable<SecurityKeys> AllGenerationsOrderedDesc()
    {
        var snapshot = _sortedSnapshot.Length == _generations.Count ? _sortedSnapshot : _generations.ToArray();
        if (snapshot.Length != _generations.Count || _sortedSnapshot.Length == 0)
        {
            snapshot = _generations.ToArray();
            Array.Sort(snapshot, (a, b) => b.Key.CompareTo(a.Key));
        }
        foreach (var (_, keys) in snapshot)
            yield return keys;
    }

    private static long EpochOf(DateTimeOffset now, TimeSpan? interval)
        => interval is { } i ? now.UtcTicks / i.Ticks : 0;

    private static SecurityKeys Derive(SecurityOptions options, long epoch)
    {
        // Эпоха входит в соль: смена ключа по расписанию даёт другой ключ.
        var salt = $"epoch:{epoch}";
        var secret = options.MasterSecret;
        byte[] Derive(string component) => Rfc2898DeriveBytes.Pbkdf2(
            secret,
            System.Text.Encoding.UTF8.GetBytes(salt + component),
            options.KdfIterations,
            HashAlgorithmName.SHA256,
            32);

        return new SecurityKeys
        {
            SigningKey = Derive("signing"),
            EncryptionKey = Derive("encryption"),
        };
    }

    private readonly record struct RotationState(int Generation, long Epoch, SecurityKeys Keys);
}
