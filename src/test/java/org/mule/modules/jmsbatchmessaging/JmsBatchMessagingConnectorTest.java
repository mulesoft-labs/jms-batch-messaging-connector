/**
 * (c) 2003-2015 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.jmsbatchmessaging;

import org.mule.DefaultMuleMessage;
import org.mule.modules.tests.ConnectorTestCase;
import org.mule.tck.junit4.FunctionalTestCase;
import org.springframework.util.Assert;
import org.junit.Test;

public class JmsBatchMessagingConnectorTest extends FunctionalTestCase {
    
    @Override
    protected String getConfigResources() {
        return "jms-batch-messaging-config.xml";
    }

    @Test
    public void testFlow() throws Exception {
		for (int i = 0; i < 1000; i++) {
			muleContext.getClient().dispatch("jms://foo",
					new DefaultMuleMessage("FOO", muleContext));
		}
		Thread.sleep(5000);
    }
}
