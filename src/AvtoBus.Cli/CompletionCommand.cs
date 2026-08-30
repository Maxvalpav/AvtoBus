using System.CommandLine;
using System.CommandLine.Parsing;

namespace AvtoBus.Cli;

public static class CompletionCommand
{
    public static Command Create()
    {
        var command = new Command("completion", "Генерация shell-автодополнения");

        var shell = new Argument<string>("shell") { HelpName = "zsh|bash|fish|powershell" };
        command.Add(shell);

        command.SetAction((parseResult, ct) =>
        {
            var shellName = parseResult.GetValue(shell) ?? "";
            var script = shellName.ToLowerInvariant() switch
            {
                "zsh" => ZshScript,
                "bash" => BashScript,
                "fish" => FishScript,
                "powershell" or "pwsh" => PowershellScript,
                _ => throw new ArgumentException($"Неизвестный shell: {shellName}. Доступно: zsh, bash, fish, powershell"),
            };
            Console.Write(script);
            return Task.FromResult(0);
        });

        return command;
    }

    private const string BashScript = """
        # avtobus bash completion
        _avtobus() {
          local cur="${COMP_WORDS[COMP_CWORD]}"
          local commands="doctor contracts es asyncapi config dlq completion"
          COMPREPLY=( $(compgen -W "$commands" -- "$cur") )
        }
        complete -F _avtobus avtobus
        """;

    private const string ZshScript = """
        #compdef avtobus
        _avtobus() {
          local -a commands
          commands=(doctor contracts es asyncapi config dlq completion)
          _describe 'command' commands
        }
        compdef _avtobus avtobus
        """;

    private const string FishScript = """
        # avtobus fish completion
        complete -c avtobus -f -a "doctor contracts es asyncapi config dlq completion"
        """;

    private const string PowershellScript = """
        # avtobus PowerShell completion
        Register-ArgumentCompleter -Native -CommandName avtobus -ScriptBlock {
          param($wordToComplete, $commandAst, $cursorPosition)
          'doctor','contracts','es','asyncapi','config','dlq','completion' |
            Where-Object { $_ -like "$wordToComplete*" } |
            ForEach-Object { [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_) }
        }
        """;
}
