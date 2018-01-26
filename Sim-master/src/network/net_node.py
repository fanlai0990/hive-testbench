class NetNode(object):
    # id : {info}
    # "n1" : {"compPower" : 10, "dataSources" : ["p1", "p2"] },
    def __init__(self, id, info):
        # static attr
        self.id = id
        self.info = info
        # self.power = info["compPower"]
        # self.partition = info["dataSources"]

        # dynamic attr
        self.compute_load = 0
        self.transmission_load = 0
        self.last_timestamp = 0
        self.arrived = dict()

    def isComputeDone(self, time):
        remainder = self.compute_load/(time-self.last_timestamp)
        if remainder > 1:
            return False
        return True

    def isTransDone(self, time):
        remainder = self.transmission_load/(time-self.last_timestamp)
        if remainder > 1:
            return False
        return True
