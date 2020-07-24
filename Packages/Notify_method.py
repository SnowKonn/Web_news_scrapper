import requests

class LineNotifier:
    TARGET_URL = 'https://notify-api.line.me/api/notify'
    TOKEN = 'Please insert your notify key'

    def __init__(self):
        print("Notifier start")

    def post_message(self, msg):
        response = requests.post(
            self.TARGET_URL,
            headers={
                'Authorization': 'Bearer ' + self.TOKEN
            },
            data={
                'message': msg
            }
        )

        return response