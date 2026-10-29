# AMQP 1.0 messaging

## Language

**Connection**: Two peers' negotiated AMQP exchange over one ordered byte stream.

**Session**: A bidirectional grouping of links with independent transfer windows.

**Link**: A unidirectional named message route between a sender and a receiver.

**Delivery**: One attempt to transfer a message across a link, possibly in multiple frames.

**Credit**: A receiver's permission for a sender to start deliveries on a link.

**Settlement**: A peer's declaration that it no longer tracks a delivery's outcome.

**Outcome**: A terminal delivery result such as accepted, rejected or released.

**Transfer**: One frame carrying all or part of a delivery's encoded message.
