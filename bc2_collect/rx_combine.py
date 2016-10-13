from sys import stderr

def main(argv, cwd):
    drugs = dict(
        (rxcui, label)
        for f in argv[2:]
        for (rxcui, label) in load(cwd / f))
    print >>stderr, 'saving', len(drugs), 'to', argv[1]
    with (cwd / argv[1]).open('wb') as out:
        for code, label in sorted(drugs.items(), key=drug_key):
            print >>out, code + ',', label


def load(p):
    print >>stderr, 'loading: ', p
    for line in p.open('rb'):
        rxcui, label = line.split(', ', 1)
        yield rxcui, label.strip()


def drug_key(item):
    rxcui, label = item
    va_class, rx_label, name = label.split(' ', 2)
    return va_class, name, int(rxcui)


if __name__ == '__main__':
    def _script():
        from sys import argv
        from pathlib import Path

        main(argv, cwd=Path('.'))
    _script()
