import json
import sys
import tqdm


def main():
    pattern = sys.argv[1]
    n = int(sys.argv[2])
    events = json.load(open(pattern.replace('*', "0")))
    with open(pattern.replace('*', ''), mode='w') as dest:
        for i in tqdm.tqdm(range(1, n + 1)):
            len_before = len(events)
            events = events + json.load(open(pattern.replace('*', str(i))))
            events.sort(key=lambda a: a['event_time'])
            for e in events[:len_before]:
                dest.write(json.dumps(e))
                dest.write('\n')
            events = events[len_before:]
        for e in events:
            dest.write(json.dumps(e))
            dest.write('\n')


if __name__ == '__main__':
    main()
