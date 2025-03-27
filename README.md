
# ⚡ loadbalancer.java by h2socket

**Simple multithreaded TCP load balancer in Java, built for speed, simplicity, and scalability.**  
🧠 Powered by Java concurrency, 🚀 designed for performance, and 🛠️ easy to customize.

## 📦 Features
- Super fast multithreaded load balancing
- Round-robin backend rotation
- Plug & play with any TCP service (HTTP, custom protocols, etc)
- Written with ❤️ by **h2socket**

## 🛠️ How to Run

```bash
javac loadbalancer.java
java loadbalancer
```

By default, it listens on `localhost:8080` and balances between:
- `localhost:9001`
- `localhost:9002`
- `localhost:9003`

## 🔄 Backend Example

You can simulate backends with netcat:

```bash
nc -l 9001
nc -l 9002
nc -l 9003
```

Then hit the load balancer:

```bash
nc localhost 8080
```

And watch the connections rotate 🔁

## 📁 File Structure

```
loadbalancer.java   # The load balancer source code
README.md           # This help file
```

## 🌍 License
MIT – do anything you want, but give credit to **h2socket**

---

Built to scale, designed to flex 💪  
Follow for more: [github.com/h2socket](https://github.com/h2socket)
