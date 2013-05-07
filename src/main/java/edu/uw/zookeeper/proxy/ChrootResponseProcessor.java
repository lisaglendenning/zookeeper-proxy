package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.util.Reference;

public class ChrootResponseProcessor extends ResponsePathProcessor implements Reference<ZNodePath> {

    public static ChrootResponseProcessor newInstance(ZNodePath chroot) {
        return new ChrootResponseProcessor(chroot);
    }

    public static class UnchrootPath implements Function<String, String> {

        private final String chroot;
        
        public UnchrootPath(String chroot) {
            super();
            this.chroot = chroot;
        }

        @Override
        public String apply(String input) {
            if (! ZNodePath.SLASH.equals(input.charAt(0))) {
                return input;
            }
            int chrootLength = chroot.length();
            if (input.length() < chrootLength) {
                throw new IllegalArgumentException();
            }
            String chrootedPath = (input.length() == chrootLength)
                    ? ZNodePath.SLASH.toString()
                            : input.substring(chrootLength);
            return chrootedPath;
        }
    }
    
    protected final ZNodePath chroot;
    
    protected ChrootResponseProcessor(ZNodePath chroot) {
        super(new UnchrootPath(chroot.toString()));
        this.chroot = chroot;
    }
    
    @Override
    public ZNodePath get() {
        return chroot;
    }
}
