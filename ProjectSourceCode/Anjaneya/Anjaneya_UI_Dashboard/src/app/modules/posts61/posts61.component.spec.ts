import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts61Component } from './posts61.component';

describe('Posts61Component', () => {
  let component: Posts61Component;
  let fixture: ComponentFixture<Posts61Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts61Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts61Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
