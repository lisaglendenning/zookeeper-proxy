package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.util.Reference;

public class ChrootRequestProcessor extends RequestPathProcessor implements Reference<ZNodeLabel.Path> {

    public static ChrootRequestProcessor newInstance(ZNodeLabel.Path chroot) {
        return new ChrootRequestProcessor(chroot);
    }

    public static class ChrootPath implements Function<ZNodeLabel.Path, ZNodeLabel.Path> {

        private final ZNodeLabel.Path chroot;
        
        public ChrootPath(ZNodeLabel.Path chroot) {
            this.chroot = chroot;
        }
        
        @Override
        public ZNodeLabel.Path apply(ZNodeLabel.Path input) {
            if (! input.isAbsolute()) {
                return input;
            }
            return (input.isRoot()) ? chroot: ZNodeLabel.Path.of(chroot, input);
        }
    }
    
    protected final ZNodeLabel.Path chroot;
    
    protected ChrootRequestProcessor(ZNodeLabel.Path chroot) {
        super(new ChrootPath(chroot));
        this.chroot = chroot;
    }
    
    @Override
    public ZNodeLabel.Path get() {
        return chroot;
    }
}
