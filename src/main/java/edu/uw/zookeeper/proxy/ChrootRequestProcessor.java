package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.util.Reference;

public class ChrootRequestProcessor extends RequestPathProcessor implements Reference<ZNodeLabel.Path> {

    public static ChrootRequestProcessor newInstance(ZNodeLabel.Path chroot) {
        return new ChrootRequestProcessor(chroot);
    }

    public static class ChrootPath implements Function<String, String> {

        private final String chroot;
        
        public ChrootPath(String chroot) {
            this.chroot = chroot;
        }
        
        @Override
        public String apply(String input) {
            if (ZNodeLabel.SLASH != input.charAt(0)) {
                return input;
            }
            return (input.length() > 1) ? chroot + input : chroot;
        }
    }
    
    protected final ZNodeLabel.Path chroot;
    
    protected ChrootRequestProcessor(ZNodeLabel.Path chroot) {
        super(new ChrootPath(chroot.toString()));
        this.chroot = chroot;
    }
    
    @Override
    public ZNodeLabel.Path get() {
        return chroot;
    }
}
