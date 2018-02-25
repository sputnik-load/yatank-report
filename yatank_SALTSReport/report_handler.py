# -*- coding: utf-8 -*-

from logging import Handler


class ReportHandler(Handler):

    messages = []

    def emit(self, record):
        self.messages.append(self.format(record))

    def get_messages(self):
        return self.messages
