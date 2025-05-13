from simgrid import Engine, Mailbox, this_actor
from collections import defaultdict
from dataclasses import dataclass
import sys


@dataclass
class FlowUpdatingMsg:
    sender: str
    flow: float
    avg: float

    def size(self) -> int:
        return sys.getsizeof(self) \
            + sys.getsizeof(self.sender) \
            + sys.getsizeof(self.flow) \
            + sys.getsizeof(self.avg)


class Peer:
    TICK_INTERVAL = 1.0
    TICK_TIMEOUT = 50.0

    def __init__(self, value: str, neighbors_ids: str = ""):
        self.name = this_actor.get_host().name
        self.value = float(value)
        self.neighbors_ids = []
        if len(neighbors_ids):
            self.neighbors_ids = neighbors_ids.split(',')

        self.neighbors = dict()
        self.flows = defaultdict(float)
        self.estimates = defaultdict(float)
        self.ticks_since_last_avg = defaultdict(float)
        self.last_avg = 0.0

        # setup mailbox
        self.mailbox = Mailbox.by_name(self.name)

    # this is called right away
    def __call__(self):
        this_actor.info(f"Peer {self.name} setup.")

        for name in self.neighbors_ids:
            self.neighbors[name] = Mailbox.by_name(name)

        self.loop()

    def loop(self):
        comm = None
        while True:
            if comm is None:
                comm = self.mailbox.get_async()

            if comm.test():
                this_actor.info(
                    f"Received message: {comm.wait().get_payload()}")
                comm = None

            self.tick()
            this_actor.sleep_for(Peer.TICK_INTERVAL)

    def tick(self):
        threshold = Engine.clock - Peer.TICK_TIMEOUT

        for neigh in self.neighbors_ids:
            if self.ticks_since_last_avg[neigh] < threshold:
                self.avg_and_send(neigh)

    def avg_and_send(self, neigh: str):
        self.neighbors_ids

        flows_sum = 0.0

        for flow in map(lambda x: self.flows[x], self.neighbors_ids):
            flows_sum += flow

        estimate = self.value - flows_sum
        avg = (self.estimates[neigh] + estimate) / 2.0

        self.last_avg = avg
        self.flows[neigh] = self.flows[neigh] + avg - self.estimates[neigh]
        self.estimates[neigh] = avg

        self.ticks_since_last_avg[neigh] = Engine.clock

        payload = FlowUpdatingMsg(self.name, self.flows[neigh], avg)
        self.neighbors[neigh].put(payload, payload.size())


if __name__ == "__main__":
    e = Engine(sys.argv)

    e.load_platform("./platforms/small_platform.xml")

    e.register_actor("peer", Peer)
    e.load_deployment("./actors.xml")

    e.run_until(10_000)

    this_actor.info("Simulation finished")
