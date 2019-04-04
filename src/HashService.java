import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashService {
    static int maxNodes = 0;
    public HashService(int maxNodes){
        this.maxNodes = maxNodes;
    }
    public static int hash(String toHash){
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        digest.reset();

        digest.update(toHash.getBytes());
        byte[] array = digest.digest();

        int hashValue =	Math.abs(new BigInteger(array).intValue() % maxNodes);
        return hashValue;
    }

}

