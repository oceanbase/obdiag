# OceanBase Diagnostic Tool - Bash Completion

_obdiag_completion() {
    local cur_word prev_word type_list
    cur_word="${COMP_WORDS[COMP_CWORD]}"
    prev_word="${COMP_WORDS[COMP_CWORD-1]}"

    case "${COMP_CWORD}" in
        1)
            type_list="--version --help config gather display analyze check rca update tool display-trace"
            COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            ;;
        2)
            case "${COMP_WORDS[1]}" in
                check)
                    type_list="run list"
                    ;;
                gather)
                    type_list="log clog slog plan_monitor stack perf sysstat obproxy_log all scene ash tabledump parameter variable dbms_xplan core"
                    ;;
                display)
                    type_list="scene"
                    ;;
                analyze)
                    type_list="log flt_trace parameter variable index_space queue memory"
                    ;;
                rca)
                    type_list="run list"
                    ;;
                tool)
                    type_list="crypto_config ai_assistant io_performance config_check"
                    ;;
                update)
                    type_list="--file --version"
                    ;;
                *)
                    type_list=""
                    ;;
            esac
            COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            ;;
        3)
            case "${COMP_WORDS[1]}" in
                gather)
                    if [ "${COMP_WORDS[2]}" = "scene" ]; then
                        type_list="list run"
                        COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
                    fi
                    ;;
                display)
                    if [ "${COMP_WORDS[2]}" = "scene" ]; then
                        type_list="list run"
                        COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
                    fi
                    ;;
                analyze)
                    if [ "${COMP_WORDS[2]}" = "parameter" ]; then
                        type_list="diff default"
                        COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
                    elif [ "${COMP_WORDS[2]}" = "variable" ]; then
                        type_list="diff"
                        COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
                    fi
                    ;;
            esac
            ;;
        *)
            # Handle common options
            case "${prev_word}" in
                --from|--to)
                    # Date/time input - no completion
                    COMPREPLY=()
                    ;;
                --since)
                    type_list="5m 10m 30m 1h 2h 6h 12h 1d 3d 7d"
                    COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
                    ;;
                --scope)
                    type_list="observer election rootservice all"
                    COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
                    ;;
                --scene)
                    # Would need dynamic scene list
                    COMPREPLY=()
                    ;;
                -c|--config)
                    # File completion
                    COMPREPLY=($(compgen -f -- "${cur_word}"))
                    ;;
                --store_dir|--report_path)
                    # Directory completion
                    COMPREPLY=($(compgen -d -- "${cur_word}"))
                    ;;
                *)
                    # Common options for most commands
                    type_list="--from --to --since --scope --grep --store_dir -c --help"
                    COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
                    ;;
            esac
            ;;
    esac
}

complete -F _obdiag_completion obdiag
