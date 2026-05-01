API Reference
=============

This reference renders the docstrings for the public authoring surface and the
helper modules intended for flow code. For task-oriented examples, start with
the author guides:

- :doc:`guides/configuring-flows`
- :doc:`guides/authoring-flow-modules`
- :doc:`guides/flow-methods`
- :doc:`guides/flow-context`
- :doc:`guides/duckdb-helpers`
- :doc:`guides/polars-helpers`
- :doc:`guides/excel-helpers`
- :doc:`guides/recipes`

The package entrypoints most flow authors import are:

- ``data_engine.Flow``
- ``data_engine.Batch``
- ``data_engine.FileRef``
- ``data_engine.FlowContext``
- ``data_engine.discover_flows``
- ``data_engine.load_flow``
- ``data_engine.run``

Top-Level Package
-----------------

.. automodule:: data_engine
   :members:
   :undoc-members:

Flow Authoring
--------------

.. automodule:: data_engine.authoring.flow
   :members:
   :undoc-members:
   :show-inheritance:

Core Flow Model
---------------

.. automodule:: data_engine.core.flow
   :members:
   :undoc-members:
   :show-inheritance:

Core Primitives
---------------

.. automodule:: data_engine.core.primitives
   :members:
   :undoc-members:
   :show-inheritance:

Core Runtime Models
-------------------

.. automodule:: data_engine.core.model
   :members:
   :undoc-members:
   :show-inheritance:

Runtime Engine
--------------

.. automodule:: data_engine.runtime.engine
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.runtime.execution
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.runtime.stop
   :members:
   :undoc-members:
   :show-inheritance:

File Watching
-------------

.. automodule:: data_engine.runtime.file_watch
   :members:
   :undoc-members:
   :show-inheritance:

Authoring Helpers
-----------------

``data_engine.helpers`` re-exports the commonly used helper functions for
flow modules. Importing from a focused helper module is still encouraged when
it keeps the flow dependency clear.

.. automodule:: data_engine.helpers
   :members:
   :undoc-members:

.. automodule:: data_engine.helpers.schema
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.helpers.polars
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.helpers.excel
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.helpers.duckdb
   :members:
   :undoc-members:
   :show-inheritance:

Host Surfaces
-------------

.. automodule:: data_engine.hosts.scheduler
   :members:
   :undoc-members:
   :show-inheritance:

Application Services
--------------------

.. automodule:: data_engine.services.daemon
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.daemon_state
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.flow_catalog
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.flow_execution
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.ledger
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.logs
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.runtime_binding
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.runtime_execution
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.runtime_history
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.settings
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.shared_state
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.theme
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.workspace_provisioning
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: data_engine.services.workspaces
   :members:
   :undoc-members:
   :show-inheritance:
