_obdiag_completion() {
    local cur_word args type_list
    cur_word="${COMP_WORDS[COMP_CWORD]}"
    args="${COMP_WORDS[@]:1}"

    case "${COMP_CWORD}" in
        1)
            type_list="--version display-trace config gather analyze check rca update"
            COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            ;;
        2)
            case "${COMP_WORDS[1]}" in
                gather)
                    if [ "$COMP_CWORD" -eq 2 ]; then
                        type_list="log clog slog plan_monitor stack perf sysstat obproxy_log all scene ash tabledump parameter variable"
                    elif [ "${COMP_WORDS[2]}" = "scene" ] && [ "$COMP_CWORD" -eq 3 ]; then
                        type_list="list run"
                    fi
                    ;;
                analyze)
                    if [ "$COMP_CWORD" -eq 2 ]; then
                        type_list="log flt_trace sql sql_review parameter variable"
                    elif [ "${COMP_WORDS[2]}" = "parameter" ] && [ "$COMP_CWORD" -eq 3 ]; then
                        type_list="diff default"
                    elif [ "${COMP_WORDS[2]}" = "variable" ] && [ "$COMP_CWORD" -eq 3 ]; then
                        type_list="diff"
                    fi
                    ;;
                rca)
                    type_list="list run"
                    ;;
                *)
                    type_list=""
                    ;;
            esac
            COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            ;;
        3)
            if [ "${COMP_WORDS[1]}" = "gather" ] && [ "${COMP_WORDS[2]}" = "scene" ]; then
                type_list="list run"
                COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            elif [ "${COMP_WORDS[1]}" = "analyze" ] && [ "${COMP_WORDS[2]}" = "parameter" ]; then
                type_list="diff default"
                COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            elif [ "${COMP_WORDS[1]}" = "analyze" ] && [ "${COMP_WORDS[2]}" = "variable" ]; then
                type_list="diff"
                COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            fi
            ;;
        *)
            COMPREPLY=()
            ;;
    esac
}

complete -F _obdiag_completion obdiag