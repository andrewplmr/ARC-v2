import logging
from engine.utils import load_config

cfg = load_config()

def send_report_via_email(to_addresses, subject, body, attachments):
    """
    Basic SMTP sending using yagmail (requires SMTP config in config.yaml).
    This function is a thin wrapper; SMTP must be enabled in config.
    """
    if not cfg.get("smtp", {}).get("enabled", False):
        logging.info("SMTP disabled in config; skipping email send.")
        return False

    try:
        import yagmail
        user = cfg["smtp"]["username"]
        pwd = cfg["smtp"]["password"]
        host = cfg["smtp"]["host"]
        yag = yagmail.SMTP(user=user, password=pwd, host=host)
        yag.send(to=to_addresses, subject=subject, contents=body, attachments=attachments)
        logging.info(f"Email sent to {to_addresses}")
        return True
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        return False
