import time
from threading import Thread

from bonobo.constants import BEGIN, END
from bonobo.strategies.base import Strategy
from bonobo.structs.bags import Bag


class ThreadCollectionStrategy(Strategy):
    def execute(self, graph, *args, plugins=None, services=None, **kwargs):
        context = self.create_graph_execution_context(graph, plugins=plugins, services=services)
        context.recv(BEGIN, Bag(), END)

        threads = []

        # for plugin_context in context.plugins:

        #    def _runner(plugin_context=plugin_context):
        #        plugin_context.start()
        #        plugin_context.loop()
        #        plugin_context.stop()

        #    futures.append(executor.submit(_runner))

        for node_context in context.nodes:
            def _runner(node_context=node_context):
                node_context.start()
                node_context.loop()

            thread = Thread(target=_runner)
            threads.append(thread)
            thread.start()

        while context.alive and len(threads):
            time.sleep(0.1)
            threads = list(filter(lambda thread: thread.is_alive, threads))

        # for plugin_context in context.plugins:
        #    plugin_context.shutdown()

        # executor.shutdown()

        return context
