/*
 * This class extends a SeekableStream and is used for random access to 
 * encrypted files. This class is not used to encrypt files. Only seek()
 * and read() functionality is implemented.
 * This class is used by the Secure HTTPS Server to deliver unencrypted
 * portions of BAM files.
 */
package uk.ac.ebi.embl.ega_cipher;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import uk.ac.ebi.embl.ega_cipher.CipherStream_256;
import net.sf.samtools.util.SeekableStream;

/**
 *
 * @author asenf
 */
public class SeekableCipherStream_256 extends SeekableStream {

    public static final int DEFAULT_BUFFER_SIZE = 512000;

    final SeekableStream wrappedStream;
    Cipher cipher;
    CipherInputStream incipher;
    long position;
    SecretKey skey_;
    byte[] orig_digest; // Original IV\
    int pw_strength;

    public SeekableCipherStream_256(SeekableStream in, char[] password, int bufferSize) {
        this(in, password, bufferSize, 256);
    }
    public SeekableCipherStream_256(SeekableStream in, char[] password, int bufferSize, int pw_strength) {
        this.wrappedStream = in;
        this.position = 0;
        this.pw_strength = pw_strength;

        // Initialization Vector
        byte[] iv = new byte[16];
        try {
            in.read(iv, 0, 16);
        } catch (IOException ex) {
            Logger.getLogger(SeekableCipherStream_256.class.getName()).log(Level.SEVERE, null, ex);
        }
        this.orig_digest = new byte[16];
        System.arraycopy(iv,0, this.orig_digest, 0, 16);

        this.cipher = CipherStream_256.getCipher(password, false, pw_strength, this.orig_digest);
        this.incipher = new CipherInputStream(this.wrappedStream, cipher);
    }
    public SeekableCipherStream_256(SeekableStream in, char[] password) {
        this(in, password, DEFAULT_BUFFER_SIZE);
    }

    @Override
    public void seek(long position) throws IOException {
        this.position = position;
        this.wrappedStream.seek(position+16);

        // Set position to start of current enclosing block
        long position1 = ( position / 16 ) * 16; // Only based on AES Block Size of 16 bytes!

        // Reset the initialization vector to the correct block
        byte[] newIV = new byte[this.orig_digest.length];
        System.arraycopy(this.orig_digest, 0, newIV, 0, this.orig_digest.length); // preserved start value
        byte_increment_fast(newIV, position1); // is there a better way??
        IvParameterSpec ivSpec_new = new IvParameterSpec(newIV);
        try {
            this.cipher.init(Cipher.ENCRYPT_MODE, this.skey_, ivSpec_new);
        } catch (InvalidKeyException ex) {
            Logger.getLogger(SeekableCipherStream_256.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvalidAlgorithmParameterException ex) {
            Logger.getLogger(SeekableCipherStream_256.class.getName()).log(Level.SEVERE, null, ex);
        }
        this.incipher = new CipherInputStream(this.wrappedStream, cipher);
    }
    private static void byte_increment_fast(byte[] data, long increment) {
        long countdown = increment / 16; // Count number of block updates
        
        ArrayList<Integer> digits_ = new ArrayList<Integer>();
        int cnt = 0; long d = 256, cn = 0;
        while (countdown > cn && d > 0) {
            int l = (int) ((countdown % d) / (d/256));
            digits_.add(l);
            cn += (l*(d/256));
            d *= 256;
        }
        int size = digits_.size();
        int[] digits = new int[size];
        for (int i=0; i<size; i++) {
            digits[size-1-i] = digits_.get(i).intValue();
        }
        
        int cur_pos = data.length-1, carryover = 0, delta = data.length-digits.length;
        
        for (int i=cur_pos; i>=delta; i--) { // Work on individual digits
            int digit = digits[i-delta] + carryover; // convert to integer
            int place = (int)(data[i]&0xFF); // convert data[] to integer
            int new_place = digit+place;
            if (new_place >= 256) carryover=1; else carryover=0;
            data[i] = (byte) (new_place % 256);
        }
        
        // Deal with potential last carryovers
        cur_pos -= digits.length;
        while (carryover == 1 && cur_pos>= 0) {
            data[cur_pos]++;
            if (data[cur_pos]==0) carryover=1; else carryover=0;
            cur_pos--;
        }
    }
    
    @Override
    public long length() {
        return this.wrappedStream.length()-16; // subtract prepended IV
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        long position1 = ( (this.position+offset) / 16 ) * 16; // Only based on AES Block Size of 16 bytes! block-correct start pos
        long p = this.position; // remember desired start
        seek(position1); // position at block-correct start to enable AES decryption

        int delta = (int) ((p+offset) - position1); // gap between block start and desired start
        byte[] buf = new byte[length+delta]; // enough to hold all data plus delta

        int n = 0; // number of read bytes
        int length1 = length + delta;
        while (n < length1) {
            //System.out.println(buf.length + "\t" + n + "\t" + (length1-n) + "\t" + length1);
            final int count = this.incipher.read(buf, n, length1-n); // read into buffer directly; offset later will be: delta
            if (count < 0) {
                System.arraycopy(buf, delta, buffer, offset, (n-delta));
                return (n-delta);
            }
            n += count;
        }
        seek(p); // Reset actual position

        System.arraycopy(buf, delta, buffer, offset, length);
        assert (n-delta == length); // must match length

        this.position += length;
        return (n-delta); 
    }

    @Override
    public void close() throws IOException {
        this.wrappedStream.close();
    }

    @Override
    public boolean eof() throws IOException {
        return this.position >= this.wrappedStream.length();
    }

    @Override
    public String getSource() {
        return this.wrappedStream.getSource();
    }

    @Override
    public int read() throws IOException {
        byte[] buf = new byte[16];
        
        long position1 = ( (this.position) / 16 ) * 16;
        long p = this.position;
        seek(position1);
        
        this.incipher.read(buf);
        
        int offset = (int)(p-position1);
        seek(p+1);
        
        return (int)(buf[offset]&0xFF); // return a proper int
    }
}
