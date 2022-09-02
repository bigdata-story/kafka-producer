import json
import sys
import tqdm


def main():
    file_name = sys.argv[1]
    dest_folder = sys.argv[2]
    s = json.load(open(file_name))
    print("file loaded")
    list_events = []
    part = 0
    for x in tqdm.tqdm(s):
        course_id, user_events = x
        for user_id, sessions in user_events.items():
            for session_id, session_events in sessions.items():
                for event in session_events:
                    list_events.append({
                        'course_id': course_id,
                        'user_id': user_id,
                        'session_id': session_id,
                        'event_type': event[0],
                        'event_time': event[1]
                    })
                    if len(list_events) == 3000000:
                        json.dump(
                            list_events,
                            fp=open(
                                f'{dest_folder}/{file_name.split(".")[0]}-converted-{part}.json',
                                mode='a+'
                            )
                        )
                        list_events = []
                        part += 1
    json.dump(
        list_events,
        fp=open(
            f'{dest_folder}/{file_name.split(".")[0]}-converted-{part}.json',
            mode='a+'
        )
    )


if __name__ == '__main__':
    main()

a = [
    '75a65336-266e-42ff-8016-4df59b3f5465',
    '6fd7c34c-9951-4b5f-9e10-16097108dc61',
    '3383bad6-69ba-4242-a125-1954482ba6df',
    '0a2a9167-f19c-4f0d-b9d7-1765c4030de7',
    'cbf30aae-8f7d-41fb-ad04-057cb7f30838',
    '9c26485b-f5a6-4c6d-97c7-2515ffdd5ab8',
    'a7e1863e-9412-4bc4-895b-2656246e676d',
    '40c85084-36f3-4545-a683-9950e5a9c7bc',
]
