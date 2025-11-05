import java.io.Serializable;
import java.util.Objects;

/**
 * Represents the multi-dimensional key for a reducer,
 * as per the Shares algorithm[cite: 190].
 * For our J5 residual join, the key is (keyA, keyD, keyE).
 */
public class ReducerKey implements Serializable, Comparable<ReducerKey> {
    private final int keyA;
    private final int keyD;
    private final int keyE;

    public ReducerKey(int keyA, int keyD, int keyE) {
        this.keyA = keyA;
        this.keyD = keyD;
        this.keyE = keyE;
    }

    // Getters
    public int getKeyA() { return keyA; }
    public int getKeyD() { return keyD; }
    public int getKeyE() { return keyE; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReducerKey that = (ReducerKey) o;
        return keyA == that.keyA && keyD == that.keyD && keyE == that.keyE;
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyA, keyD, keyE);
    }

    @Override
    public String toString() {
        return "(" + keyA + ", " + keyD + ", " + keyE + ')';
    }

    @Override
    public int compareTo(ReducerKey o) {
        int aCmp = Integer.compare(this.keyA, o.keyA);
        if (aCmp != 0) return aCmp;
        int dCmp = Integer.compare(this.keyD, o.keyD);
        if (dCmp != 0) return dCmp;
        return Integer.compare(this.keyE, o.keyE);
    }
}