package main

import (
	"fmt"
	"os"
)

// bashCompletion also backs zsh, via bashcompinit — the well-known
// cross-shell trick, avoids maintaining two near-identical scripts.
const bashCompletion = `_nucladb_cli() {
  local cur cmds
  cur="${COMP_WORDS[COMP_CWORD]}"
  cmds="quickstart create-tenant insert batch-upsert search delete ping completion"
  if [ "$COMP_CWORD" -eq 1 ]; then
    COMPREPLY=( $(compgen -W "$cmds" -- "$cur") )
    return
  fi
  case "${COMP_WORDS[1]}" in
    create-tenant) COMPREPLY=( $(compgen -W "-id -max-vectors -max-qps -json" -- "$cur") ) ;;
    insert)        COMPREPLY=( $(compgen -W "-id -vector -tenant -meta -json" -- "$cur") ) ;;
    batch-upsert)  COMPREPLY=( $(compgen -W "-file -tenant -json" -- "$cur") ) ;;
    search)        COMPREPLY=( $(compgen -W "-vector -top-k -ef -tenant -filter -json" -- "$cur") ) ;;
    delete)        COMPREPLY=( $(compgen -W "-id -tenant -json" -- "$cur") ) ;;
    ping)          COMPREPLY=( $(compgen -W "-json" -- "$cur") ) ;;
    completion)    COMPREPLY=( $(compgen -W "bash zsh fish" -- "$cur") ) ;;
  esac
}
complete -F _nucladb_cli nucladb-cli
`

const zshCompletion = `autoload -Uz bashcompinit && bashcompinit
` + bashCompletion

const fishCompletion = `complete -c nucladb-cli -n "__fish_use_subcommand" -a quickstart -d "Try every command against a throwaway local server"
complete -c nucladb-cli -n "__fish_use_subcommand" -a create-tenant -d "Provision a new tenant"
complete -c nucladb-cli -n "__fish_use_subcommand" -a insert -d "Insert or update a vector"
complete -c nucladb-cli -n "__fish_use_subcommand" -a batch-upsert -d "Insert or update many vectors from a file"
complete -c nucladb-cli -n "__fish_use_subcommand" -a search -d "Find nearest neighbors"
complete -c nucladb-cli -n "__fish_use_subcommand" -a delete -d "Delete a vector by id"
complete -c nucladb-cli -n "__fish_use_subcommand" -a ping -d "Check the server is reachable"
complete -c nucladb-cli -n "__fish_use_subcommand" -a completion -d "Print a shell completion script"
complete -c nucladb-cli -n "__fish_seen_subcommand_from create-tenant" -l id -l max-vectors -l max-qps -l json
complete -c nucladb-cli -n "__fish_seen_subcommand_from insert" -l id -l vector -l tenant -l meta -l json
complete -c nucladb-cli -n "__fish_seen_subcommand_from batch-upsert" -l file -l tenant -l json
complete -c nucladb-cli -n "__fish_seen_subcommand_from search" -l vector -l top-k -l ef -l tenant -l filter -l json
complete -c nucladb-cli -n "__fish_seen_subcommand_from delete" -l id -l tenant -l json
complete -c nucladb-cli -n "__fish_seen_subcommand_from ping" -l json
complete -c nucladb-cli -n "__fish_seen_subcommand_from completion" -a "bash zsh fish"
`

func runCompletion(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("completion: usage: nucladb-cli completion <bash|zsh|fish>")
	}
	var script, howto string
	switch args[0] {
	case "bash":
		script = bashCompletion
		howto = `echo 'source <(nucladb-cli completion bash)' >> ~/.bashrc`
	case "zsh":
		script = zshCompletion
		howto = `echo 'source <(nucladb-cli completion zsh)' >> ~/.zshrc`
	case "fish":
		script = fishCompletion
		howto = `nucladb-cli completion fish > ~/.config/fish/completions/nucladb-cli.fish`
	default:
		return fmt.Errorf("completion: unknown shell %q (want bash, zsh, or fish)", args[0])
	}
	fmt.Fprintf(os.Stderr, "# add this to your shell config, then restart your shell:\n#   %s\n", howto)
	fmt.Print(script)
	return nil
}
