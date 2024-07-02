## analyze sql

```bash
$ obdiag analyze sql [options]

Options:
  --host=HOST          tenant connection host
  --port=PORT          tenant connection port
  --password=PASSWORD  tenant connection user password
  --user=USER          tenant connection user name
  --from=FROM          specify the start of the time range. format: 'yyyy-mm-
                       dd hh:mm:ss'
  --to=TO              specify the end of the time range. format: 'yyyy-mm-dd
                       hh:mm:ss'
  --since=SINCE        Specify time range that from 'n' [d]ays, 'n' [h]ours or
                       'n' [m]inutes. before to now. format: <n> <m|h|d>.
                       example: 1h.
  --level=LEVEL        The alarm level, optional parameters [critical, warn,
                       notice, ok]
  -c C                 obdiag custom config
  -h, --help           Show help and exit.
  -v, --verbose        Activate verbose output.
```
