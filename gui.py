import threading
import queue
import time
import paramiko
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from tkinter import scrolledtext


# ------------------------------
# SSH session helper (background)
# ------------------------------
class SSHSession:
    """
    Handles the paramiko SSH connection and background receiving of data.
    Received text is put into self.recv_queue for the GUI to read safely.
    """

    def __init__(self, recv_queue=None):
        self.client = None
        self.shell = None
        self.recv_thread = None
        self.stop_event = threading.Event()
        # Queue where incoming terminal lines are placed for GUI
        self.recv_queue = recv_queue if recv_queue is not None else queue.Queue()

    def connect(self, hostname, port, username, password, timeout=10):
        """
        Attempt to connect to an SSH server and start a PTY shell.
        Returns (True, None) on success or (False, error_message) on failure.
        """
        try:
            # Create client and auto-accept host key (simple behavior)
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect (paramiko handles authentication)
            self.client.connect(hostname=hostname, port=int(port),
                                username=username, password=password, timeout=timeout)

            # Open a shell (interactive pty)
            self.shell = self.client.invoke_shell()
            self.shell.settimeout(0.0)  # non-blocking reads

            # Clear any previous stop event and start receiving thread
            self.stop_event.clear()
            self.recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
            self.recv_thread.start()

            return True, None
        except Exception as e:
            # Clean up on error
            self.close()
            return False, str(e)

    def _recv_loop(self):
        """
        Loop running in background thread to read shell output.
        We use non-blocking recv and collect available data repeatedly.
        All output is pushed into self.recv_queue for the GUI to handle.
        """
        try:
            while not self.stop_event.is_set():
                if self.shell is None:
                    break
                try:
                    if self.shell.recv_ready():
                        # Read up to 4096 bytes
                        data = self.shell.recv(4096)
                        if not data:
                            # Connection closed at remote side
                            self.recv_queue.put("\n[connection closed by remote]\n")
                            break
                        # decode and put into queue
                        text = data.decode(errors="ignore")
                        self.recv_queue.put(text)
                    else:
                        # nothing ready; sleep briefly
                        time.sleep(0.05)
                except Exception:
                    # If socket times out or another read error happens, sleep briefly
                    time.sleep(0.05)
                    continue
        finally:
            # ensure cleanup
            self.close()

    def send(self, data: str):
        """Send raw data to the shell (must include newline if needed)."""
        try:
            if self.shell is not None:
                self.shell.send(data)
            else:
                raise RuntimeError("Shell is not available.")
        except Exception as e:
            # Inform GUI via queue
            self.recv_queue.put(f"\n[send error] {e}\n")

    def close(self):
        """Stop receive loop and close shell and client."""
        try:
            self.stop_event.set()
        except Exception:
            pass
        try:
            if self.shell is not None:
                self.shell.close()
        except Exception:
            pass
        try:
            if self.client is not None:
                self.client.close()
        except Exception:
            pass
        self.shell = None
        self.client = None


# ------------------------------
# Main GUI Application
# ------------------------------
class App:
    def __init__(self, root):
        self.root = root
        root.title("SSH + pbrun Automator")
        root.geometry("900x600")

        # Queue for incoming terminal data
        self.recv_queue = queue.Queue()

        # Create SSH session object (it places incoming text into recv_queue)
        self.sess = SSHSession(recv_queue=self.recv_queue)

        # Build the GUI (top form, buttons, terminal, input)
        self._build_form()
        # Start periodic check to pull text from recv_queue to the GUI
        self._schedule_poll_recv()

    def _build_form(self):
        frm = ttk.Frame(self.root, padding=8)
        frm.grid(row=0, column=0, sticky="nsew")
        # allow resizing
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)
        frm.rowconfigure(4, weight=1)  # terminal row grows

        # --- Row 0: host / port / user / password ---
        ttk.Label(frm, text="Host").grid(row=0, column=0, sticky="w")
        self.host_e = ttk.Entry(frm, width=20)
        self.host_e.grid(row=0, column=1, sticky="w")

        ttk.Label(frm, text="Port").grid(row=0, column=2, sticky="w", padx=(10, 0))
        self.port_e = ttk.Entry(frm, width=6)
        self.port_e.insert(0, "22")
        self.port_e.grid(row=0, column=3, sticky="w")

        ttk.Label(frm, text="SSH Username").grid(row=0, column=4, sticky="w", padx=(10, 0))
        self.user_e = ttk.Entry(frm, width=20)
        self.user_e.grid(row=0, column=5, sticky="w")

        ttk.Label(frm, text="SSH Password").grid(row=0, column=6, sticky="w", padx=(10, 0))
        self.pass_e = ttk.Entry(frm, width=20, show="*")
        self.pass_e.grid(row=0, column=7, sticky="w")

        # --- Row 1: pbrun target and pbrun password ---
        ttk.Label(frm, text="pbrun Target User").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.target_e = ttk.Entry(frm, width=20)
        self.target_e.grid(row=1, column=1, sticky="w", pady=(6, 0))

        ttk.Label(frm, text="pbrun Password").grid(row=1, column=2, sticky="w", padx=(10, 0), pady=(6, 0))
        self.pbrun_pass_e = ttk.Entry(frm, width=20, show="*")
        self.pbrun_pass_e.grid(row=1, column=3, sticky="w", pady=(6, 0))

        # --- Row 2: Buttons: Connect, Run pbrun -> shell, Disconnect ---
        self.connect_btn = ttk.Button(frm, text="Connect", command=self.on_connect)
        self.connect_btn.grid(row=2, column=0, pady=8)

        self.pbrun_btn = ttk.Button(frm, text="Run pbrun -> shell", command=self.on_pbrun, state="disabled")
        self.pbrun_btn.grid(row=2, column=1, padx=(6, 0), pady=8)

        self.disconnect_btn = ttk.Button(frm, text="Disconnect", command=self.on_disconnect, state="disabled")
        self.disconnect_btn.grid(row=2, column=2, padx=(6, 0), pady=8)

        # --- Row 4: Terminal output (scrolled text) ---
        self.terminal = scrolledtext.ScrolledText(frm, wrap="char", width=120, height=24, state="disabled")
        self.terminal.grid(row=4, column=0, columnspan=8, sticky="nsew", pady=(6, 0))

        # --- Row 5: Input line and Send button ---
        self.cmd_entry = ttk.Entry(frm, width=100)
        self.cmd_entry.grid(row=5, column=0, columnspan=6, sticky="we", pady=(6, 0))
        self.cmd_entry.bind("<Return>", self.on_send_cmd)

        self.send_btn = ttk.Button(frm, text="Send", command=self.on_send_cmd)
        self.send_btn.grid(row=5, column=6, sticky="w", padx=(6, 0), pady=(6, 0))

    # -------------------
    # Terminal helpers
    # -------------------
    def _append_to_terminal(self, text: str):
        """Safely append text to the scrolled text widget (must be called from main thread)."""
        if not text:
            return
        self.terminal.configure(state="normal")
        self.terminal.insert("end", text)
        self.terminal.see("end")
        self.terminal.configure(state="disabled")

    def _schedule_poll_recv(self):
        """
        Periodically called in the GUI main thread to flush recv_queue into the terminal.
        This keeps GUI updates in the main thread and avoids thread-safety issues.
        """
        try:
            while True:
                text = self.recv_queue.get_nowait()
                self._append_to_terminal(text)
        except queue.Empty:
            pass
        # call this method again after 100ms
        self.root.after(100, self._schedule_poll_recv)

    # -------------------
    # Button callbacks
    # -------------------
    def on_connect(self):
        host = self.host_e.get().strip()
        port = self.port_e.get().strip() or "22"
        user = self.user_e.get().strip()
        pwd = self.pass_e.get().strip()
        if not (host and user and pwd):
            messagebox.showwarning("Missing", "Host, SSH username and password are required")
            return

        # disable connect button during attempt
        self.connect_btn.config(state="disabled")
        self.terminal.configure(state="normal")
        self.terminal.delete("1.0", "end")
        self.terminal.configure(state="disabled")

        def worker():
            ok, err = self.sess.connect(host, port, user, pwd)
            if not ok:
                self.recv_queue.put(f"\nConnection failed: {err}\n")
                # re-enable connect button
                self.connect_btn.config(state="normal")
                return

            # connected successfully -> enable pbrun & disconnect
            self.recv_queue.put(f"\nConnected to {host} as {user}\n")
            self.pbrun_btn.config(state="normal")
            self.disconnect_btn.config(state="normal")

        threading.Thread(target=worker, daemon=True).start()

    def on_pbrun(self):
        """
        Send the pbrun command to start a shell as another user, then wait briefly for
        a password prompt and send the pbrun password automatically.
        """
        target = self.target_e.get().strip()
        pbrun_pwd = self.pbrun_pass_e.get().strip()
        if not target:
            messagebox.showwarning("Missing", "Please supply the pbrun target user")
            return

        # form the command (adjust if your system uses different pbrun syntax)
        # Example: pbrun -u <target> bash -l
        cmd = f"pbrun -u {target} bash\n"
        self.sess.send(cmd)
        self.recv_queue.put(f"\nRunning: {cmd}")

        # Wait in separate thread for a password prompt
        def waiter():
            timeout = 8
            start = time.time()
            seen_buff = ""
            while time.time() - start < timeout:
                try:
                    chunk = self.recv_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                # accumulate small output and check for password prompt keywords
                seen_buff += chunk.lower()
                if ("password:" in seen_buff) or ("password for" in seen_buff):
                    # send pbrun password + newline
                    if pbrun_pwd:
                        self.sess.send(pbrun_pwd + "\n")
                        self.recv_queue.put("\n[pbrun password sent]\n")
                    else:
                        self.recv_queue.put("\n[pbrun password prompt seen but no password supplied]\n")
                    return
            # timeout without seeing password prompt
            self.recv_queue.put("\n[pbrun password prompt not detected - password not sent]\n")

        threading.Thread(target=waiter, daemon=True).start()

    def on_send_cmd(self, event=None):
        cmd = self.cmd_entry.get()
        if not cmd:
            return
        # ensure newline
        if not cmd.endswith("\n"):
            cmd = cmd + "\n"
        self.sess.send(cmd)
        # clear entry
        self.cmd_entry.delete(0, "end")

    def on_disconnect(self):
        self.sess.close()
        self.recv_queue.put("\n[connection closed]\n")
        self.connect_btn.config(state="normal")
        self.pbrun_btn.config(state="disabled")
        self.disconnect_btn.config(state="disabled")


# ------------------------------
# Run the app
# ------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
