from simgrid import Engine, Mailbox, this_actor
import sys


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
        self.flows = dict()
        self.estimates = dict()
        self.ticks_since_last_avg = dict()
        self.last_avg = 0.0

        # setup mailbox
        self.mailbox = Mailbox.by_name(self.name)

        self.last_tick = 0.0

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
        if self.last_tick < (Engine.clock - Peer.TICK_TIMEOUT):
            self.last_tick = Engine.clock


if __name__ == "__main__":
    e = Engine(sys.argv)

    e.load_platform("./platforms/small_platform.xml")

    e.register_actor("peer", Peer)
    e.load_deployment("./actors.xml")

    e.run_until(10_000)

    this_actor.info("Simulation finished")
