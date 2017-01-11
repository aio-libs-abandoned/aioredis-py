.. module:: aioredis.pubsub

:mod:`aioredis.pubsub` module
=============================

Module provides a Pub/Sub listener interface implementing
multi-producers, single-consumer queue pattern.


.. autoclass:: Receiver
   :members:

   To do: few words regarding exclusive channel usage.


.. autoclass:: _Sender

   Bases: :class:`aioredis.abc.AbcChannel`

   **Not to be used directly**, returned by :meth:`Receiver.channel` or
   :meth:`Receiver.pattern()` calls.
