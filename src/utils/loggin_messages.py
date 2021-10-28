import logging

default = 'Error message not founded'

def error_messages(message_code):
    """Return a message log in console

    Args:
        message_code (int): Mesagge code to retrive in console from the dict messages

    Raises:
        logging.error: Error if don't match any message or any message code is passed
    """
    messages = {
        1: 'Source not founded'
    }

    message = messages.get(message_code)
    if message:
        logging.error(message)
    else:
        raise logging.error(default)

def info_messages(message_code=0, auto=True, message=None):
    """Show info message in console

    Args:
        message_code (int, optional): Mesagge code to retrive in console from the dict messages. Defaults to 0.
        auto (bool, optional): If auto is set, a info message is raised to console. Defaults to True.
        message (str, optional): If auto is False this message will be showed. Defaults to None.

    Raises:
        logging.error: Error if don't match any message or any message code is passed
    """
    messages = {
        0: 'Info message empty'
    }
    if auto == False and message != None:
        logging.info(message)
    else:
        message = messages.get(message_code)
        if message:
            logging.error(message)
        else:
            raise logging.error(default)

