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
    TICK_TIMEOUT = 50

    def __init__(self, value: str, neighbors_ids_str: str = ""):
        self.name = this_actor.get_host().name
        self.value = float(value)
        neighbors_ids = []
        if len(neighbors_ids_str):
            neighbors_ids = neighbors_ids_str.split(",")

        self.flows = defaultdict(float)
        self.estimates = defaultdict(float)
        self.msg_recvd_ids = set()
        self.ticks_since_last_avg = 0
        self._last_avg = 0.0
        self.neighbors = dict()
        for name in neighbors_ids:
            self.neighbors[name] = Mailbox.by_name(name)

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
        this_actor.info(f"Peer {self.name} with value {self.value} started.")
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
        self.ticks_since_last_avg += 1

        if self.ticks_since_last_avg >= Peer.TICK_TIMEOUT:
            self.avg_and_send()

    def on_receive(self, msg: FlowUpdatingMsg):
        if msg.sender not in self.neighbors.keys():
            self.neighbors[msg.sender] = Mailbox.by_name(msg.sender)
            this_actor.error(f"{msg.sender} was not {self.name}'s neighbor")

        self.estimates[msg.sender] = msg.estimate
        self.flows[msg.sender] = -msg.flow
        self.msg_recvd_ids.add(msg.sender)

        if self.msg_recvd_ids.issuperset(self.neighbors.keys()):
            self.avg_and_send()

    def avg_and_send(self):
        flows_sum = sum(map(lambda x: self.flows[x], self.neighbors.keys()))
        estimate = self.value - flows_sum

        avg_sum = 0.0
        for est in map(lambda n: self.estimates[n], self.neighbors.keys()):
            avg_sum += est

        avg = (estimate + avg_sum) / (len(self.neighbors.keys()) + 1)
        self.last_avg = avg

        for name in self.neighbors.keys():
            new_flow = self.flows[name] + avg - self.estimates[name]
            self.flows[name] = new_flow
            self.estimates[name] = avg

            payload = FlowUpdatingMsg(self.name, new_flow, avg)
            self.pending_comms.push(
                self.neighbors[name].put_async(payload, payload.size())
            )

        self.msg_recvd_ids = set()
        self.ticks_since_last_avg = 0


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
    Actor.create("watcher", Host.by_name("observer"), watcher, 1000.0, 10.0)

    e.run_until(10000)

    this_actor.info("Simulation finished")
