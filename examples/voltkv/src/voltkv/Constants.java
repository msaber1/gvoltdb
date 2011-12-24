package voltkv;

public class Constants {
	public static final byte ROW_UNLOCKED = 1;
    public static final byte EXPIRED_LOCK_MAY_NEED_REPLAY= 2;
    public static final byte ROW_LOCKED= 3;
    public static final byte EXPIRE_TIME_REACHED = 4;
    public static final byte KEY_DOES_NOT_EXIST = 5;
}
