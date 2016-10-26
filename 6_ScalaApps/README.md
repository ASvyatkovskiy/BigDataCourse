# Enable Scala syntax highlighting

Following bash one-liner will enable Scala syntax highlighting in vim:

```bash
mkdir -p ~/.vim/{ftdetect,indent,syntax} && for d in ftdetect indent syntax ; do wget --no-check-certificate -O ~/.vim/$d/scala.vim https://raw.githubusercontent.com/derekwyatt/vim-scala/master/syntax/scala.vim; done
```
