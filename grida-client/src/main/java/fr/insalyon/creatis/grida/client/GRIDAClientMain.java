/* Copyright CNRS-CREATIS
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
package fr.insalyon.creatis.grida.client;

import java.net.Socket;
import java.net.UnknownHostException;
import java.io.IOException;

import static fr.insalyon.creatis.grida.common.ExecutorConstants.*;
import fr.insalyon.creatis.grida.common.bean.CachedFile;
import fr.insalyon.creatis.grida.common.bean.GridData;
import fr.insalyon.creatis.grida.common.bean.Operation;
import fr.insalyon.creatis.grida.common.bean.ZombieFile;
import java.io.File;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class GRIDAClientMain {
    public static void main(String[] args) {
        GRIDAClientMain main = new GRIDAClientMain();
        ClientOptions options = main.handleArgs(args);

        try {
            String result = main.executeCommand(options);
            System.out.println(result);
        } catch (GRIDAClientException gce) {
            gce.printStackTrace(System.err);
        }
    }

    private ClientOptions handleArgs(String[] args) {
        Options options = new Options();

        options.addOption(
            "h", "host", true, "host of the server (default localhost)");
        options.addOption(
            "p", "port", true, "port of the server (default 9006)");
        options.addOption(
            "r",
            "proxy",
            true,
            "path of the user's proxy file (default $X509_USER_PROXY)");

        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cmd = parser.parse(options, args);

            String host = cmd.getOptionValue('h', "localhost");
            int port = Integer.parseInt(cmd.getOptionValue('p', "9006"));
            String proxy = cmd.getOptionValue(
                'r', System.getenv("X509_USER_PROXY"));

            if (!isProxyFileExisting(proxy)) {
                printErrorAndExit(
                    "Proxy file does not exist: " + proxy, options);
            }

            String[] remainingArgs = cmd.getArgs();
            if (remainingArgs.length < 1) {
                printErrorAndExit("Missing command", options);
            }

            String command = remainingArgs[0];
            String[] cmdOptions =
                Arrays.copyOfRange(remainingArgs, 1, remainingArgs.length);

            return new ClientOptions(host, port, proxy, command, cmdOptions);
        } catch(ParseException e) {
            printErrorAndExit(e.getMessage(), options);
        } catch(NumberFormatException e) {
            printErrorAndExit("Should be a number, " + e.getMessage(), options);
        }
        return null; // To satisfy the compiler.  We don't pass here.
    }

    private void printErrorAndExit(String error, Options options) {
        System.err.println(error);
        printUsage(options);
        System.exit(1);
    }

    private void printUsage(Options options) {
        String commands =
            "\n" +
            "<command> is one of the following (case insensitive):\n" +
            " getFile <remoteFile> <localDir>\n" +
            " getFolder <remoteDir> <localDir>\n" +
            " list <dir> <1 if refresh, or else 0>\n" +
            " getModDate <filename>\n" +
            " upload <localFile> <remoteDir>\n" +
            " uploadToSes <localFile> <remoteDir> <storageElement>\n" +
            " replicate <remoteFile>\n" +
            " delete <path to file or dir>\n" +
            " createFolder <path>\n" +
            " rename <oldPath> <newPath>\n" +
            " exists <remotePath>\n" +
            " setComment [lfn:]<path> <rev>\n" +
            " listWithComment <dir> <1 if refresh, or else 0>\n" +
            " cacheList\n" +
            " cacheDelete <path>\n" +
            " poolAdd <localFile> <remoteDir> <user>\n" +
            " poolById <id>\n" +
            " poolByUser <user>\n" +
            " poolRemoveById <id>\n" +
            " poolRemoveByUser <user>\n" +
            " poolAll\n" +
            " poolByDate <user> <limit> <startDate>\n" +
            " zombieGet\n" +
            " zombieDelete <surl>\n";

        new HelpFormatter().printHelp(
            "gridaClient [options] <command> <args>",
            "Options:",
            options,
            commands);
    }

    private String executeCommand(ClientOptions options)
        throws GRIDAClientException {

        if (!isNumberOfArgumentsCorrect(options)) {
            System.exit(1);
        }

        // Both clients are used in many commands, so they are created
        // globally.  Impact is low if they are finally not used.
        GRIDAClient client = new GRIDAClient(
            options.host, options.port, options.proxy);
        GRIDAPoolClient poolClient = new GRIDAPoolClient(
            options.host, options.port, options.proxy);

        String firstArg = options.cmdOptions.length == 0
            ? null
            : options.cmdOptions[0];

        String result = "Done.";
        switch (options.command.toLowerCase()) {
        case "getfile":
            result = client.getRemoteFile(firstArg, options.cmdOptions[1]);
            break;
        case "getfolder":
            result = client.getRemoteFolder(firstArg, options.cmdOptions[1]);
            break;
        case "list":
        {
            boolean refresh = options.cmdOptions[1].equals("1");
            result = list(client, firstArg, refresh, false);
        }
        break;
        case "getmoddate":
            result = Long.toString(client.getModificationDate(firstArg));
            break;
        case "upload":
            result = client.uploadFile(firstArg, options.cmdOptions[1]);
            break;
        case "uploadtoses":
            result = client.uploadFileToSE(
                firstArg, options.cmdOptions[1], options.cmdOptions[2]);
            break;
        case "replicate":
            client.replicateToPreferredSEs(firstArg);
            break;
        case "delete":
            client.delete(Arrays.asList(firstArg));
            break;
        case "createfolder":
            int index = firstArg.lastIndexOf('/');
            String path = index > 0
                ? firstArg.substring(0, index)
                : "";
            String name = firstArg.substring(index + 1);
            client.createFolder(path, name);
            break;
        case "rename":
            client.rename(firstArg, options.cmdOptions[1]);
            break;
        case "exists":
            result = Boolean.toString(client.exist(firstArg));
            break;
        case "setcomment":
            client.setComment(firstArg, options.cmdOptions[1]);
            break;
        case "listwithcomment":
        {
            boolean refresh = options.cmdOptions[1].equals("1");
            result = list(client, firstArg, refresh, true);
        }
        break;
        case "cachelist":
        {
            GRIDACacheClient cacheClient = new GRIDACacheClient(
                options.host, options.port, options.proxy);
            StringBuilder sb = new StringBuilder();
            for (CachedFile cf: cacheClient.getCachedFiles()) {
                sb.append(
                    cf.getPath() + ' ' +
                    cf.getName() + ' ' +
                    cf.getSize() + ' ' +
                    cf.getFrequency() + ' ' +
                    cf.getLastUsage() +
                    '\n');
            }
            result = sb.toString();
        }
        break;
        case "cachedelete":
        {
            GRIDACacheClient cacheClient = new GRIDACacheClient(
                options.host, options.port, options.proxy);
            cacheClient.deleteCachedFile(firstArg);
        }
        break;
        case "pooladd":
            result = poolClient.uploadFile(
                firstArg, options.cmdOptions[1], options.cmdOptions[2]);
            break;
        case "poolbyid":
            result = poolClient.getOperationById(firstArg).toString();
            break;
        case "poolbyuser":
        {
            StringBuilder sb = new StringBuilder();
            for (Operation op: poolClient.getOperationsListByUser(firstArg)) {
                sb.append(op.toString()).append('\n');
            }
            result = sb.toString();
        }
        break;
        case "poolremovebyid":
            poolClient.removeOperationById(firstArg);
            break;
        case "poolremovebyuser":
            poolClient.removeOperationsByUser(firstArg);
            break;
        case "poolall":
        {
            StringBuilder sb = new StringBuilder();
            for (Operation op: poolClient.getAllOperations()) {
                sb.append(op.toString()).append('\n');
            }
            result = sb.toString();
        }
        break;
        case "poolbydate":
        {
            try {
                StringBuilder sb = new StringBuilder();
                Date date = DateFormat.getDateInstance().parse(
                    options.cmdOptions[2]);
                for (Operation op: poolClient.getOperationsLimitedListByUserAndDate(
                         firstArg,
                         Integer.parseInt(options.cmdOptions[1]),
                         date)) {
                    sb.append(op.toString()).append('\n');
                }
                result = sb.toString();
            } catch (java.text.ParseException pe) {
                result = "Wrong date format: " + options.cmdOptions[2];
            }
        }
        break;
        case "zombieget":
        {
            GRIDAZombieClient zombieClient = new GRIDAZombieClient(
                options.host, options.port, options.proxy);
            StringBuilder sb = new StringBuilder();
            for (ZombieFile zf: zombieClient.getList()) {
                sb.append(zf.getSurl())
                    .append(zf.getRegistration().toString())
                    .append('\n');
            }
            result = sb.toString();
        }
        break;
        case "zombiedelete":
        {
            GRIDAZombieClient zombieClient = new GRIDAZombieClient(
                options.host, options.port, options.proxy);
            zombieClient.delete(firstArg);
        }
        break;
        default:
            System.err.println("Unknown command: " + options.command);
            System.exit(1);
        }
        return result;
    }

    private boolean isProxyFileExisting(String proxy) {
        return proxy != null &&
            new File(proxy).exists() &&
            new File(proxy).isFile();
    }

    private boolean isNumberOfArgumentsCorrect(ClientOptions options) {
        String[] commands = {
            "getfile",
            "getfolder",
            "list",
            "getmoddate",
            "upload",
            "uploadtoses",
            "replicate",
            "delete",
            "createfolder",
            "rename",
            "exists",
            "setcomment",
            "listwithcomment",
            "cachelist",
            "cachedelete",
            "pooladd",
            "poolbyid",
            "poolbyuser",
            "poolremovebyid",
            "poolremovebyuser",
            "poolall",
            "poolbydate",
            "zombieget",
            "zombiedelete"
        };

        int[] nbNeededArguments = {
            2, // "getfile",
            2, // "getfolder",
            2, // "list",
            1, // "getmoddate",
            2, // "upload",
            3, // "uploadtoses",
            1, // "replicate",
            1, // "delete",
            1, // "createfolder",
            2, // "rename",
            1, // "exists",
            2, // "setcomment",
            2, // "listwithcomment",
            0, // "cachelist",
            1, // "cachedelete",
            3, // "pooladd",
            1, // "poolbyid",
            1, // "poolbyuser",
            1, // "poolremovebyid",
            1, // "poolremovebyuser",
            0, // "poolall",
            3, // "poolbydate",
            0, // "zombieget",
            1 // "zombiedelete"
        };

        if (commands.length != nbNeededArguments.length) {
            System.err.println(
                "Sanity check failed: " +
                "commands.length != nbNeededArguments.length");
            System.exit(1);
        }

        int lg = commands.length;
        String command = options.command.toLowerCase();
        for (int i = 0; i < lg; i++) {
            if (commands[i].equals(command)) {
                boolean isOk =
                    nbNeededArguments[i] == options.cmdOptions.length;
                if (!isOk) {
                    System.err.println(
                        "Wrong number of arguments for command " +
                        options.command +
                        ". Found " + options.cmdOptions.length +
                        ", Needed " + nbNeededArguments[i] +
                        ".");
                }
                return isOk;
            }
        }
        System.err.println("Unknown command: " + options.command);
        return false;
    }

    private String list(
        GRIDAClient client, String dir, boolean refresh, boolean withComment)
        throws GRIDAClientException {
        StringBuilder sb = new StringBuilder();
        for (GridData gd: client.getFolderData(dir, refresh)) {
            sb.append(
                gd.getName() + ' ' +
                gd.getType() + ' ' +
                gd.getLength() + ' ' +
                gd.getModificationDate() + ' ' +
                gd.getPermissions() + ' ' +
                gd.getReplicas());
            if (withComment) {
                sb.append(' ' + gd.getComment());
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static class ClientOptions {
        public final String host;
        public final int port;
        public final String proxy;
        public final String command;
        public final String[] cmdOptions;

        public ClientOptions(
            String host,
            int port,
            String proxy,
            String command,
            String[] cmdOptions) {
            this.host = host;
            this.port = port;
            this.proxy = proxy;
            this.command = command;
            this.cmdOptions = cmdOptions;
        }
    }
}
