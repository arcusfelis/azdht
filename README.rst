
azDHT node in Erlang
====================

This protocol is used by Vuze.
http://wiki.vuze.com/w/Distributed_hash_table

Unlike mainline-DHT this protocol is harder to detect with DPI, because azDHT
does not use bencoding.

**License**: MIT

**Author**: Uvarov Michael (freeakk@gmail.com)


.. image:: https://secure.travis-ci.org/arcusfelis/azdht.png?branch=master
    :alt: Build Status
    :target: http://travis-ci.org/arcusfelis/azdht


Test on Mac
=====

Requires more loopback addresses:

.. code:: bash
    sudo ifconfig lo0 alias 127.0.0.2 up
    sudo ifconfig lo0 alias 127.0.0.3 up
    sudo ifconfig lo0 alias 127.0.0.4 up

 Otherwise you will get error:

 .. code:: erlang
         {failed_to_start_child,azdht_net,
          {{open_socket_failed,
            #{ip => {127,0,0,2},
              port => 43301,
              reason => {error,eaddrnotavail}}},
