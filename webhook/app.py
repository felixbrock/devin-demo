"""
Webhook receiver for GitHub issue events
Receives GitHub webhook signals and forwards them to the operator

Triggers when the target label (TARGET_LABEL, default 'devin-fix') is added
to a GitHub issue. Verifies the webhook HMAC signature when
GITHUB_WEBHOOK_SECRET is configured.
"""

from flask import Flask, request, jsonify
import hashlib
import hmac
import os
import requests

app = Flask(__name__)

OPERATOR_URL = os.getenv('OPERATOR_URL', 'http://operator:8001')
TARGET_LABEL = os.getenv('TARGET_LABEL', 'devin-fix')
WEBHOOK_SECRET = os.getenv('GITHUB_WEBHOOK_SECRET', '')


def verify_signature(payload_body, signature_header):
    """Verify the GitHub webhook HMAC-SHA256 signature"""
    if not WEBHOOK_SECRET:
        app.logger.warning('GITHUB_WEBHOOK_SECRET not set - skipping signature verification')
        return True
    if not signature_header:
        return False
    expected = 'sha256=' + hmac.new(
        WEBHOOK_SECRET.encode('utf-8'), payload_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)


@app.route('/webhook', methods=['POST'])
def github_webhook():
    """
    Receive GitHub webhook events

    Only 'issues' events with action 'labeled' and the target label
    are forwarded to the operator; everything else is acknowledged
    and ignored.
    """
    if not verify_signature(request.get_data(), request.headers.get('X-Hub-Signature-256')):
        return jsonify({'error': 'Invalid webhook signature'}), 401

    if request.headers.get('X-GitHub-Event') != 'issues':
        return jsonify({'status': 'ignored', 'reason': 'not an issues event'}), 200

    try:
        data = request.json

        if data.get('action') != 'labeled':
            return jsonify({'status': 'ignored', 'reason': 'not a labeled action'}), 200

        label = data.get('label', {})
        if label.get('name') != TARGET_LABEL:
            return jsonify({'status': 'ignored', 'reason': f'label is not {TARGET_LABEL}'}), 200

        issue = data.get('issue', {})
        operator_payload = {
            'issue_id': issue.get('id'),
            'issue_number': issue.get('number'),
            'issue_title': issue.get('title'),
            'issue_body': issue.get('body') or '',
            'issue_url': issue.get('html_url'),
            'issue_labels': [l.get('name') for l in issue.get('labels', [])],
            'repository': data.get('repository', {}).get('full_name'),
            'action': data.get('action'),
            'sender': data.get('sender', {}).get('login')
        }

        response = requests.post(f'{OPERATOR_URL}/issues', json=operator_payload, timeout=10)
        return jsonify({'status': 'forwarded', 'operator_response': response.json()}), 200

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Operator service unavailable: {str(e)}'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
