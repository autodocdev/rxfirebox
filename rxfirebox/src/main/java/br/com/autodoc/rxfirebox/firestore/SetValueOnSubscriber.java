package br.com.autodoc.rxfirebox.firestore;


import androidx.annotation.NonNull;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.firestore.DocumentReference;

import br.com.autodoc.rxfirebox.Executor;
import io.reactivex.CompletableEmitter;
import io.reactivex.CompletableOnSubscribe;

public class SetValueOnSubscriber implements CompletableOnSubscribe {

    private final Object value;
    private final DocumentReference reference;
    private final boolean useListener;

    public SetValueOnSubscriber(Object value, DocumentReference reference, boolean useListener) {
        this.value = value;
        this.reference = reference;
        this.useListener = useListener;
    }

    @Override
    public void subscribe(CompletableEmitter e) throws Exception {
      if(useListener) {
          reference.set(value).addOnCompleteListener(Executor.Companion.executeThreadPoolExecutor(), new RxCompletionListener(e));
      }else {
          reference.set(value);
          e.onComplete();
      }
    }

    private static class RxCompletionListener implements OnCompleteListener<Void> {

        private final CompletableEmitter subscriber;

        public RxCompletionListener(CompletableEmitter subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void onComplete(@NonNull Task<Void> task) {
            if(task.getException() != null){
                subscriber.onError(task.getException());
            }else {
                subscriber.onComplete();
            }
        }
    }
}
