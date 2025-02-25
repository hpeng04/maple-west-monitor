from alert import send_email

# Monthly alert for failed downloads by emailing failed_downloads.txt to recipients
# then clearing the file.

def alert_failed_downloads(path):
    with open(path, 'r') as f:
        subject = 'Maple West Failed Downloads'
        body = f.read()
        send_email(subject, body, attachment=path)
    with open(path, 'w') as f:
        f.truncate(0)
    return

if __name__ == '__main__':
    alert_failed_downloads('failed_downloads.txt')