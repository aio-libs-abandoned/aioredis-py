.. module:: aioredis.pubsub

:mod:`aioredis.pubsub` --- Pub/Sub Tools Reference
==================================================

Module provides a Pub/Sub listener interface implementing
multi-producers, single-consumer queue pattern.


.. autoclass:: Receiver
   :members:

   .. warning::

      Currently subscriptions implementation has few issues that will
      be solved eventually, but until then developer must be aware of the
      following:

      * Single ``Receiver`` instance can not be shared between two (or more)
        connections (or client instances) because any of them can close
        ``_Sender``.

      * Several ``Receiver`` instances can not subscribe to the same
        channel or pattern. This is a flaw in subscription mode implementation:
        subsequent subscriptions to some channel always return first-created
        Channel object.


.. autoclass:: _Sender

   Bases: :class:`aioredis.abc.AbcChannel`

   **Not to be used directly**, returned by :meth:`Receiver.channel` or
   :meth:`Receiver.pattern()` calls.
