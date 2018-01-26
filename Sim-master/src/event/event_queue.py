class EventQueue(object):
    def __init__(self, init_event_list=[]):
        self.queue = []
        for e in init_event_list:
            self.insert(e)

    def insert(self, event):
        for i in range(len(self.queue)):
            if event.timestamp < self.queue[i][0].timestamp:
                self.queue.insert(i, [event])
                return
            elif event.timestamp == self.queue[i][0].timestamp:
                self.queue[i].append(event)
                return

        self.queue.append([event])

    def pop(self):
        assert (len(self.queue) > 0), "queue size 0!"
        el = self.queue[0]
        del self.queue[0]
        return el

    def empty(self):
        return len(self.queue) == 0

    @property
    def timestamp(self):
        """
        :return: first event in the queue to be processed
        """
        if self.empty():
            return 0
        return self.queue[0][0].timestamp

    def __str__(self):
        return "\n".join(map(lambda x: "event_queue_item: ["+"\n".join(map(lambda y: str(y), x)) + "]", self.queue))