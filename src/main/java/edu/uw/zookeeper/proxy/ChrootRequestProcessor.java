package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.util.Reference;

public class ChrootRequestProcessor extends RequestPathProcessor implements Reference<ZNodeName.Path> {

    public static ChrootRequestProcessor newInstance(ZNodeName.Path chroot) {
        return new ChrootRequestProcessor(chroot);
    }

    public static class ChrootPath implements Function<String, String> {

        private final String chroot;
        
        public ChrootPath(String chroot) {
            this.chroot = chroot;
        }
        
        @Override
        public String apply(String input) {
            if (! ZNodeName.Path.SLASH.equals(input.charAt(0))) {
                return input;
            }
            return (input.length() > 1) ? chroot + input : chroot;
        }
    }
    
    protected final ZNodeName.Path chroot;
    
    protected ChrootRequestProcessor(ZNodeName.Path chroot) {
        super(new ChrootPath(chroot.toString()));
        this.chroot = chroot;
    }
    
    @Override
    public ZNodeName.Path get() {
        return chroot;
    }
}
