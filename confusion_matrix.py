import matplotlib.pyplot as plt
import numpy as np

cm = np.array([   
    [ 2,  5,  0],
    [ 6, 47,  4],
    [ 0,  6,  1]
])

classes = ["Negative", "Neutral", "Positive"]

plt.figure(figsize=(6, 5))
plt.imshow(cm, interpolation='nearest', cmap="Blues") 
plt.title("Confusion Matrix")
plt.colorbar()

for i in range(cm.shape[0]):
    for j in range(cm.shape[1]):
        plt.text(j, i, str(cm[i, j]),
                 ha="center", va="center")

plt.xticks(np.arange(len(classes)), classes)
plt.yticks(np.arange(len(classes)), classes)

plt.xlabel("Predicted Label")
plt.ylabel("True Label")

plt.tight_layout()
plt.savefig("cm_dataincomplete.png", dpi=300)
plt.show()