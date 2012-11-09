/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.ebi.embl.ega_cipher;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 *
 * @author asenf
 */
public class CipherInputStream_256 {
    private Cipher cipher;
    private InputStream is;
    private int pw_strength;
    
    public CipherInputStream_256(InputStream in, char[] password, int pw_strength) {
        this.pw_strength = pw_strength;
        
        try {
            this.is = new BufferedInputStream(in);
            
            // Key Generation
            byte[] salt = {(byte)-12, (byte)34, (byte)1, (byte)0, (byte)-98, (byte)223, (byte)78, (byte)21};                
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            KeySpec spec = new PBEKeySpec(password, salt, 1024, this.pw_strength); // Password Strength - n bits
            SecretKey tmp = factory.generateSecret(spec);
            SecretKey secret = new SecretKeySpec(tmp.getEncoded(), "AES");

            // Initialization Vector
            byte[] random_iv = new byte[16];
            try {
                this.is.read(random_iv, 0, 16);
            } catch (IOException ex) {
                Logger.getLogger(CipherInputStream_256.class.getName()).log(Level.SEVERE, null, ex);
            }
            AlgorithmParameterSpec paramSpec = new IvParameterSpec(random_iv);

            // Cipher Setup
            cipher = Cipher.getInstance("AES/CTR/NoPadding"); // load a cipher AES / Segmented Integer Counter
            cipher.init(Cipher.DECRYPT_MODE, secret, paramSpec);

        } catch (InvalidAlgorithmParameterException ex) {
            Logger.getLogger(CipherInputStream_256.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvalidKeySpecException ex) {
            Logger.getLogger(CipherInputStream_256.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvalidKeyException ex) {
            Logger.getLogger(CipherInputStream_256.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchPaddingException ex) {
            Logger.getLogger(CipherInputStream_256.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(CipherInputStream_256.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public InputStream getCipherInputStream() {
        CipherInputStream in = null;        
        in = new CipherInputStream(is, cipher);
        
        return in;
    }
}
