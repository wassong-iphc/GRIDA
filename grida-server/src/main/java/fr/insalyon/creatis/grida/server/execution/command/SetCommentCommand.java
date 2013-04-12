/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.insalyon.creatis.grida.server.execution.command;

import fr.insalyon.creatis.grida.server.execution.Command;
import fr.insalyon.creatis.grida.common.Communication;
import fr.insalyon.creatis.grida.server.business.BusinessException;
import fr.insalyon.creatis.grida.server.business.OperationBusiness;

/**
 *
 * @author glatard
 */
public class SetCommentCommand extends Command{
    
    private String lfn;
    private String revision;
    private OperationBusiness operationBusiness;
    
    public SetCommentCommand(Communication communication, String proxyFileName,
            String lfn, String revision) {

        super(communication, proxyFileName);
        this.lfn = lfn;
        this.revision = revision;

        operationBusiness = new OperationBusiness(proxyFileName);
    }


    @Override
    public void execute() {
           try {
            operationBusiness.setComment(lfn, revision);

        } catch (BusinessException ex) {
            communication.sendErrorMessage(ex.getMessage());
        }
        communication.sendEndOfMessage();
    }
    
}
