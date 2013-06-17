package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.util.Reference;

public class ChrootResponseProcessor extends ResponsePathProcessor implements Reference<ZNodeLabel.Path> {

    public static ChrootResponseProcessor newInstance(ZNodeLabel.Path chroot) {
        return new ChrootResponseProcessor(chroot);
    }

    public static class UnchrootPath implements Function<ZNodeLabel.Path, ZNodeLabel.Path> {

        private final ZNodeLabel.Path chroot;
        
        public UnchrootPath(ZNodeLabel.Path chroot) {
            super();
            this.chroot = chroot;
        }

        @Override
        public ZNodeLabel.Path apply(ZNodeLabel.Path input) {
            if (! input.isAbsolute()) {
                return input;
            }
            if (! chroot.prefixOf(input)) {
                throw new IllegalArgumentException(input.toString());
            }
            if (chroot.equals(input)) {
                return ZNodeLabel.Path.root();
            } else {
                return ZNodeLabel.Path.of(input.toString().substring(chroot.length()));
            }
        }
    }
    
    protected final ZNodeLabel.Path chroot;
    
    protected ChrootResponseProcessor(ZNodeLabel.Path chroot) {
        super(new UnchrootPath(chroot));
        this.chroot = chroot;
    }
    
    @Override
    public ZNodeLabel.Path get() {
        return chroot;
    }
}
