package org.qcri.rheem.truthdiscovery.logical;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.logicallayer.LogicalOperatorWrapper;

/**
 * Created by zoi on 1/2/15.
 */
public class LoopWrapper extends LogicalOperatorWrapper<Boolean> {

    Loop logOp;

    public LoopWrapper(Loop logOp) {
        this.logOp = logOp;
    }

    @Override
    public Boolean apply(RheemContext context, Object... args) {
        return this.logOp.condition(args[0], context);
    }
}
