/**
 * Shared delayed close for navbar menu hover: moving from the hamburger to the
 * dropdown must not immediately close the panel.
 */

let closeTimer: ReturnType<typeof setTimeout> | undefined;

export function clearNavbarMenuCloseTimer(): void {
  if (closeTimer !== undefined) {
    clearTimeout(closeTimer);
    closeTimer = undefined;
  }
}

/**
 * If `shouldClose()` is still true after the delay, calls `toggle()` once.
 * Typical use: shouldClose reads latest "sidebar open" state via a ref.
 */
export function scheduleNavbarMenuClose(
  shouldClose: () => boolean,
  toggle: () => void,
  delayMs = 280,
): void {
  clearNavbarMenuCloseTimer();
  closeTimer = setTimeout(() => {
    closeTimer = undefined;
    if (shouldClose()) {
      toggle();
    }
  }, delayMs);
}
