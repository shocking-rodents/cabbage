CHANGELOG
=========

0.6.0 (2018-09-07)
-----

- Python 3.7 support
- aiompq==0.11
- Added tests for amqp.py

0.5.3 (2018-07-02)
------------------

- Fix duplication messages with unexpected correlation id  

0.5.2 (2018-06-05)
------------------

- Added blocking mechanism to assure that connection is ready to work

0.5.1 (2018-03-27)
------------------

- Removed dependency imports from `setup.py`.

0.5 (2018-03-27)
----------------

- Support for cycling through multiple hosts.
- Per-subscription request handlers.
- Stop consuming and wait for request handlers to finish on shutdown.

0.4 (2018-03-12)
----------------

- Raw mode for request handlers that use binary protocols.
- Sensible default values for many arguments.
