from simgrid import Actor, ActivitySet, Mailbox, Engine, Host, this_actor
from collections import defaultdict
from dataclasses import dataclass
import sys


@dataclass
class FlowUpdatingMsg:
    sender: str
    flow: float
    estimate: float

    def size(self) -> int:
        return (
            sys.getsizeof(self)
            + sys.getsizeof(self.sender)
            + sys.getsizeof(self.flow)
            + sys.getsizeof(self.estimate)
        )


class Peer:
    TICK_INTERVAL = 1.0
    TICK_TIMEOUT = 50.0

    def __init__(self, value: str, neighbors_ids: str = ""):
        self.name = this_actor.get_host().name
        self.value = float(value)
        self.neighbors_ids = []
        if len(neighbors_ids):
            self.neighbors_ids = neighbors_ids.split(",")

        self.neighbors = dict()
        self.flows = defaultdict(float)
        self.estimates = defaultdict(float)
        self.ticks_since_last_avg = defaultdict(float)
        self._last_avg = 0.0

        # setup mailbox
        self.mailbox = Mailbox.by_name(self.name)

        self.pending_comms = ActivitySet()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        global_values["value"][self.name] = val
        self._value = val

    @property
    def last_avg(self):
        return self._last_avg

    @last_avg.setter
    def last_avg(self, val):
        global_values["last_avg"][self.name] = val
        self._last_avg = val

    # this is called right away
    def __call__(self):
        this_actor.info(f"Peer {self.name} with value {self.value} setup.")

        for name in self.neighbors_ids:
            self.neighbors[name] = Mailbox.by_name(name)

        self.loop()

    def loop(self):
        comm = None
        while True:
            if comm is None:
                comm = self.mailbox.get_async()

            if comm.test():
                msg = comm.wait().get_payload()
                comm.cancel()
                comm = None

                if type(msg) is FlowUpdatingMsg:
                    self.on_receive(msg)

            self.tick()
            this_actor.sleep_for(Peer.TICK_INTERVAL)

    def tick(self):
        threshold = Engine.clock - Peer.TICK_TIMEOUT

        for neigh in self.neighbors_ids:
            if self.ticks_since_last_avg[neigh] < threshold:
                self.avg_and_send(neigh)

    def on_receive(self, msg: FlowUpdatingMsg):
        if msg.sender not in self.neighbors.keys():
            self.neighbors[msg.sender] = Mailbox.by_name(msg.sender)
            this_actor.error(f"{msg.sender} is not {self.name}'s neighbor")

        self.estimates[msg.sender] = msg.estimate
        self.flows[msg.sender] = -msg.flow
        self.avg_and_send(msg.sender)

    def avg_and_send(self, neigh: str):
        flows_sum = sum(map(lambda x: self.flows[x], self.neighbors_ids))
        estimate = self.value - flows_sum
        avg = (self.estimates[neigh] + estimate) / 2.0

        self.last_avg = avg
        self.flows[neigh] = self.flows[neigh] + avg - self.estimates[neigh]
        self.estimates[neigh] = avg

        self.ticks_since_last_avg[neigh] = Engine.clock

        payload = FlowUpdatingMsg(self.name, self.flows[neigh], avg)
        self.pending_comms.push(
            self.neighbors[neigh].put_async(payload, payload.size())
        )


global_values = defaultdict(dict)


def print_global_values():
    for key in global_values.keys():
        this_actor.info(f"{key}{global_values[key]}")


def watcher(run_until: float, time_interval: float = 10.0):
    while Engine.clock < run_until:
        this_actor.sleep_for(min(time_interval, run_until - Engine.clock))
        print_global_values()

    this_actor.info("Killing every actor but myself.")
    Actor.kill_all()

    # this line is not really necessary
    this_actor.exit()


if __name__ == "__main__":
    e = Engine(sys.argv)

    e.load_platform("./platforms/small_platform.xml")

    e.register_actor("peer", Peer)
    e.load_deployment("./actors.xml")

    e.netzone_root.add_host("observer", 25e6)

    # Add a watcher of the changes
    Actor.create("watcher", Host.by_name("observer"), watcher, 1000.0, 15.0)

    e.run_until(10000)

    this_actor.info("Simulation finished")
