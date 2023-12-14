
_obdiag_completion() {
    local cur_word args type_list
    cur_word="${COMP_WORDS[COMP_CWORD]}"
    args="${COMP_WORDS[@]:1}"

    case "${COMP_CWORD}" in
        1)
            type_list="version display-trace config gather analyze check"
            COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            ;;
        2)
            case "${COMP_WORDS[1]}" in
                gather)
                    type_list="log clog slog plan_monitor stack perf obproxy_log all"
                    ;;
                analyze)
                    type_list="log flt_trace"
                    ;;
                *)
                    type_list=""
                    ;;
            esac
            COMPREPLY=($(compgen -W "${type_list}" -- "${cur_word}"))
            ;;
        *)
            COMPREPLY=()
            ;;
    esac
}

complete -F _obdiag_completion obdiag