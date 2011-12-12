/* Copyright CNRS-CREATIS
 *
 * Rafael Silva
 * rafael.silva@creatis.insa-lyon.fr
 * http://www.creatis.insa-lyon.fr/~silva
 *
 * This software is a grid-enabled data-driven workflow manager and editor.
 *
 * This software is governed by the CeCILL  license under French law and
 * abiding by the rules of distribution of free software.  You can  use,
 * modify and/ or redistribute the software under the terms of the CeCILL
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and  rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty  and the software's author,  the holder of the
 * economic rights,  and the successive licensors  have only  limited
 * liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading,  using,  modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean  that it is complicated to manipulate,  and  that  also
 * therefore means  that it is reserved for developers  and  experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systems and/or
 * data to be ensured and,  more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license and that you accept its terms.
 */
package fr.insalyon.creatis.agent.vlet.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;

/**
 *
 * @author Rafael Silva
 */
public class VletAgentClientTest extends TestCase {
    
    public VletAgentClientTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test of getRemoteFile method, of class VletAgentClient.
     */
    public void testGetRemoteFile() throws Exception {
//        System.out.println("getRemoteFile");
//        String remoteFile = "/grid/biomed/creatis/test.txt";
//        String localDir = "/tmp";
//        VletAgentClient instance = new VletAgentClient("iv-tg-351.creatis.insa-lyon.fr", 9006, "/tmp/x509up_u501");
//        String expResult = "/tmp/test.txt";
//        String result = instance.getRemoteFile(remoteFile, localDir);
//        assertEquals(expResult, result);
    }

    /**
     * Test of getDirectoriesList method, of class VletAgentClient.
     */
    public void testGetDirectoriesList() throws Exception {
//        System.out.println("getDirectoriesList");
//        String dir = "/grid/biomed/creatis/rafael";
//        VletAgentClient instance = new VletAgentClient("iv-tg-351.creatis.insa-lyon.fr", 9006, "/tmp/x509up_u501");
//        String[] res = new String[]{"gasw", "mktest2", "test-sorteo", "test-sorteo-2", "test.sh"};
//        List expResult = new ArrayList<String>();
//        expResult = Arrays.asList(res);
//        List result = instance.getDirectoriesList(dir);
//        assertEquals(expResult, result);
    }

    /**
     * Test of getFilesList method, of class VletAgentClient.
     */
    public void testGetFilesList() throws Exception {
//        System.out.println("getDirectoriesList");
//        String dir = "/grid/biomed/creatis/rafael";
//        VletAgentClient instance = new VletAgentClient("iv-tg-351.creatis.insa-lyon.fr", 9006, "/tmp/x509up_u501");
//        String[] res = new String[]{"merge_test.xml", "protocole.txt.proto-uploadTest"};
//        List expResult = new ArrayList<String>();
//        expResult = Arrays.asList(res);
//        List result = instance.getFilesList(dir);
//        assertEquals(expResult, result);
    }

    /**
     * Test of getModificationDate method, of class VletAgentClient.
     */
    public void testGetModificationDate() throws Exception {
//        System.out.println("getModificationDate");
//
//        String[] res = new String[]{"/grid/biomed/creatis/test.xml"};
//        List<String> filesList = Arrays.asList(res);;
//        VletAgentClient instance = new VletAgentClient("iv-tg-351.creatis.insa-lyon.fr", 9006, "/tmp/x509up_u501");
//
//        String expResult = "ERROR: No such file or directory";
//        try {
//            instance.getModificationDate(filesList);
//            fail("The test should launch an exception.");
//
//        } catch (VletAgentClientException ex) {
//            assertEquals(expResult, ex.getMessage());
//        }
    }

}
