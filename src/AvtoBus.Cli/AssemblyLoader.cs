using System.Reflection;
using System.Runtime.Loader;

namespace AvtoBus.Cli;

/// <summary>
/// Безопасная загрузка сборок для сканирования контрактов: только локальные .dll/.exe,
/// без URL и без исполняемого кода в default-контексте. Сборка грузится в collectible
/// AssemblyLoadContext — файл не лочится, типы сканирования не пачкают процесс CLI
/// (module initializers чужой DLL в default-контексте — произвольный код).
/// Решение: user-supplied путь в CLI считается доверенным указанием, но изоляция
/// и белые расширения убирают классы «подмена DLL» и «удалённая загрузка».
/// </summary>
public static class AssemblyLoader
{
    private static readonly string[] AllowedExtensions = [".dll", ".exe"];

    public static Assembly LoadContractsAssembly(string? path)
    {
        if (path is null)
            return typeof(IBus).Assembly;

        // Схему проверяем ДО GetFullPath: иначе URL превратится в локальный путь
        // и проверка бессмысленна.
        if (path.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"Загрузка сборок по URL запрещена: {path}");

        var full = Path.GetFullPath(path);

        var ext = Path.GetExtension(full);
        if (!AllowedExtensions.Contains(ext, StringComparer.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                $"Недопустимое расширение '{ext}': сканируются только {string.Join("/", AllowedExtensions)}.");

        if (!File.Exists(full))
            throw new FileNotFoundException($"Сборка не найдена: {full}");

        var context = new CollectibleScanContext();
        using var stream = File.OpenRead(full);
        return context.LoadFromStream(stream);
    }

    private sealed class CollectibleScanContext() : AssemblyLoadContext(isCollectible: true);
}
