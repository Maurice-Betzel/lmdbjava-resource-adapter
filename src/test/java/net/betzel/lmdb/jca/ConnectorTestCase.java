/*
    Copyright 2017 Maurice Betzel

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package net.betzel.lmdb.jca;

import java.util.UUID;
import java.util.logging.Logger;

import javax.annotation.Resource;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

/**
 * ConnectorTestCase
 *
 * @version $Revision: $
 */
@RunWith(Arquillian.class)
public class ConnectorTestCase
{
   private static Logger log = Logger.getLogger(ConnectorTestCase.class.getName());

   private static String deploymentName = "ConnectorTestCase";

   /**
    * Define the deployment
    *
    * @return The deployment archive
    */
   @Deployment
   public static ResourceAdapterArchive createDeployment()
   {
      ResourceAdapterArchive raa =
         ShrinkWrap.create(ResourceAdapterArchive.class, deploymentName + ".rar");
      JavaArchive ja = ShrinkWrap.create(JavaArchive.class, UUID.randomUUID().toString() + ".jar");
      ja.addPackages(true, Package.getPackage("net.betzel.lmdb.jca"));
      raa.addAsLibrary(ja);

      raa.addAsManifestResource("META-INF/ironjacamar.xml", "ironjacamar.xml");

      return raa;
   }

   /** Resource */
   @Resource(mappedName = "java:/eis/LMDbConnectionFactory")
   private LMDbConnectionFactory connectionFactory1;

   /**
    * Test getConnection
    *
    * @exception Throwable Thrown if case of an error
    */
   @Test
   public void testGetConnection1() throws Throwable
   {
      assertNotNull(connectionFactory1);
      LMDbConnection connection1 = connectionFactory1.getConnection();
      assertNotNull(connection1);
      connection1.close();
   }

}
