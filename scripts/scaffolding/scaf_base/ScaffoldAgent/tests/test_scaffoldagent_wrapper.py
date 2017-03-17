# import time
# import unittest
#
# from volttron.tests.platform_wrapper import PlatformWrapper
#
# """
# Test
# """
# AGENT_DIR = "Agents/ScaffoldAgentAgent"
# CONFIG_FILE = "Agents/ScaffoldAgentAgent/config"
# # AGENT_NAME = "scaffoldagentagent-0.1"
# # WHEEL_NAME = "scaffoldagentagent-0.1-py2-none-any.whl"
#
# class ScaffoldAgentTests(unittest.TestCase):
#
#     def setUp(self):
#         self.platform = PlatformWrapper()
#         self.platform.startup_platform("base-platform-test.json", use_twistd=False)
#
#     def tearDown(self):
#         self.platform.cleanup()
#
#     def test_direct_install_start_stop_start(self):
#         uuid = self.platform.direct_build_install_run_agent(AGENT_DIR, CONFIG_FILE)
#         time.sleep(5)
#         self.platform.direct_stop_agent(uuid)
#         time.sleep(5)
#         self.platform.direct_start_agent(uuid)
