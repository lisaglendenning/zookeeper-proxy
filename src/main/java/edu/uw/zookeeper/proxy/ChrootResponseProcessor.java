package edu.uw.zookeeper.proxy;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.util.Reference;

public class ChrootResponseProcessor extends ResponsePathProcessor implements Reference<ZNodeLabel.Path> {

    public static ChrootResponseProcessor newInstance(ZNodeLabel.Path chroot) {
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
            if (ZNodeLabel.Path.SLASH != input.charAt(0)) {
                return input;
            }
            int chrootLength = chroot.length();
            if (input.length() < chrootLength) {
                throw new IllegalArgumentException();
            }
            String chrootedPath = (input.length() == chrootLength)
                    ? ZNodeLabel.Path.root().toString()
                            : input.substring(chrootLength);
            return chrootedPath;
        }
    }
    
    protected final ZNodeLabel.Path chroot;
    
    protected ChrootResponseProcessor(ZNodeLabel.Path chroot) {
        super(new UnchrootPath(chroot.toString()));
        this.chroot = chroot;
    }
    
    @Override
    public ZNodeLabel.Path get() {
        return chroot;
    }
}
