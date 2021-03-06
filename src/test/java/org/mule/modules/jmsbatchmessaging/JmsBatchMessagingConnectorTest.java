/**
 * (c) 2003-2015 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.jmsbatchmessaging;

import org.junit.Test;
import org.mule.DefaultMuleMessage;
import org.mule.tck.junit4.FunctionalTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JmsBatchMessagingConnectorTest extends FunctionalTestCase {

    @Override
    protected String getConfigResources() {
        return "jms-batch-messaging-config.xml";
    }

    @Test
    public void testFlow() throws Exception {
        for (int i = 0; i < 10; i++) {
            muleContext.getClient().dispatch("jms://foo",
                    new DefaultMuleMessage("FOO", muleContext));
        }

        List<Object> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            messages.add(muleContext.getClient().request("jms://completed", 5000));
        }

        assertEquals(5, messages.size());
    }
}
