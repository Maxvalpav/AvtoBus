using BenchmarkDotNet.Running;

// Запуск всех бенчмарков: dotnet run -c Release --project benchmarks/AvtoBus.Benchmarks
// Точечный фильтр: dotnet run -c Release --project benchmarks/AvtoBus.Benchmarks -- --filter *Publish*
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
