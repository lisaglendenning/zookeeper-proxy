package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.util.Reference;

public class ChrootResponseProcessor extends ResponsePathProcessor implements Reference<ZNodeName.Path> {

    public static ChrootResponseProcessor newInstance(ZNodeName.Path chroot) {
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
            if (! ZNodeName.Path.SLASH.equals(input.charAt(0))) {
                return input;
            }
            int chrootLength = chroot.length();
            if (input.length() < chrootLength) {
                throw new IllegalArgumentException();
            }
            String chrootedPath = (input.length() == chrootLength)
                    ? ZNodeName.Path.SLASH.toString()
                            : input.substring(chrootLength);
            return chrootedPath;
        }
    }
    
    protected final ZNodeName.Path chroot;
    
    protected ChrootResponseProcessor(ZNodeName.Path chroot) {
        super(new UnchrootPath(chroot.toString()));
        this.chroot = chroot;
    }
    
    @Override
    public ZNodeName.Path get() {
        return chroot;
    }
}
