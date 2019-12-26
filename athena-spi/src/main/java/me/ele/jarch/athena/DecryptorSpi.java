package me.ele.jarch.athena;

/**
 * This class defines the *Service Provider Interface (SPI)* for various Decryptor which use for decrypt db password.
 * All the abstract methods in this class must be implemented by each service provider who wishes to supply a Decryptor implementation.
 */
public abstract class DecryptorSpi {
    /**
     * Initialize the decryptor.
     * eg: read secret from file, web, remote service.
     * call once before use.
     */
    public abstract void init();

    /**
     * Decrypt the input.
     *
     * @param input encrypted text.
     * @return decryptred text.
     */
    public abstract String decrypt(final String input);
}
