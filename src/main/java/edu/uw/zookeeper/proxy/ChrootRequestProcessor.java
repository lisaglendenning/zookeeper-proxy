package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.util.Reference;

public class ChrootRequestProcessor extends RequestPathProcessor implements Reference<ZNodePath> {

    public static ChrootRequestProcessor newInstance(ZNodePath chroot) {
        return new ChrootRequestProcessor(chroot);
    }

    public static class ChrootPath implements Function<String, String> {

        private final String chroot;
        
        public ChrootPath(String chroot) {
            super();
            this.chroot = chroot;
        }

        @Override
        public String apply(String input) {
            if (! ZNodePath.SLASH.equals(input.charAt(0))) {
                return input;
            }
            String realPath = (input.length() > 1)
                ? chroot + input : chroot;
            return realPath;
        }
    }
    
    protected final ZNodePath chroot;
    
    protected ChrootRequestProcessor(ZNodePath chroot) {
        super(new ChrootPath(chroot.toString()));
        this.chroot = chroot;
    }
    
    @Override
    public ZNodePath get() {
        return chroot;
    }
}
